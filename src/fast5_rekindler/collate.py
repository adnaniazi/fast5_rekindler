"""
Combines information from POD5 and BAM file for each read into a single 
data structure for convenient access.
"""
from loguru import logger
from capfinder.process_pod5 import pull_read_from_pod5, get_pod5_filepath, find_base_locs_in_signal, open_database, extract_roi_signal
from capfinder.bam import get_total_records, process_bam_records
from capfinder.find_ote_train import process_read as extract_roi_coords_train
from capfinder.find_ote_test import process_read as extract_roi_coords_test
from capfinder.plot import plot_roi_signal
from capfinder.align import align
from dataclasses import dataclass
from mpire import WorkerPool  # type: ignore
from typing import Dict, Any, TextIO
import os
import contextlib
import multiprocessing
import csv
import yappi 

# Create a lock for synchronization
lock = multiprocessing.Lock()

# Shared dictionary
shared_dict = multiprocessing.Manager().dict()
shared_dict['good_reads_count'] = 0
shared_dict['bad_reads_count'] = 0
shared_dict['good_reads_dir'] = 0
shared_dict['bad_reads_dir'] = 0

@dataclass
class FASTQRecord:
    """
    Simulates a FASTQ record object.
    
    Params:
        id: str
            Read ID.
        seq: str
            Read sequence.
            
    Returns:
        FASTQRecord: FASTQRecord
            A FASTQRecord object.
    """
    id: str
    seq: str
    
class DatabaseHandler:

    def __init__(self, database_path, csv_filepath):
        self.database_path = database_path
        self.csv_filepath = csv_filepath

    def init_func(self, worker_state: Dict[str, Any]) -> None:
        """Initializes the database connection for each worker."""
        worker_state['db_connection'], worker_state['db_cursor'] = open_database(self.database_path)
        # Write the header row to the CSV file
        if self.csv_filepath:
            csvfile = open(self.csv_filepath, 'a', newline='')  
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(["read_id", "plot_filepath"])
            worker_state['csv_writer'] = csv_writer
            worker_state['csvfile'] = csvfile


    def exit_func(self, worker_state: Dict[str, Any]) -> None:
        """Closes the database connection for each worker."""
        conn = worker_state['db_connection']
        conn.close()
        # Close the CSV file
        if self.csv_filepath:
            csvfile= worker_state.get('csvfile')
            csvfile.close()
        
def collate_bam_pod5_worker(worker_state: Dict[str, Any], bam_data: Dict[str, Any], reference: str, cap0_pos: int, train_or_test: str, plot_signal, output_dir: str):
    """Worker function that collates information from POD5 and BAM file, finds the 
    FASTA coordinates of  region of interest (ROI) and and extracts its signal.
    
    Params:
        worker_state: dict  
            Dictionary containing the database connection and cursor.
        bam_data: dict
            Dictionary containing the BAM record information.
        reference: str  
            Reference sequence.
        cap0_pos: int   
            Position of the cap0 base in the reference sequence.
        train_or_test: str  
            Whether to extract ROI for training or testing.
        plot_signal: bool
            Whether to plot the ROI signal.
        output_dir: str 
            Path to the output directory.
    
    Returns:
        roi_data: dict
            Dictionary containing the ROI signal and fasta sequence.
    
    """
    # Get read_id from bam record
    read_id = bam_data["read_id"]

    # Find the pod5 filepath for the read_id from index database
    pod5_filepath = get_pod5_filepath(read_id, worker_state['db_cursor'])
    
    # Pull the read data from the multi-pod5 file
    pod5_data = pull_read_from_pod5(read_id, pod5_filepath)
    
    # Extract the locations of each new base in signal coordinates
    base_locs_in_signal = find_base_locs_in_signal(bam_data)
    
    # Simulate a FASTQ record object
    read_fasta = bam_data["read_fasta"]
    fastq_record = FASTQRecord(read_id, read_fasta)
    
    # Get ROI
    if train_or_test.lower() == "train": 
        aln_res = extract_roi_coords_train(record=fastq_record, reference=reference, cap0_pos=cap0_pos) 
    elif train_or_test.lower() == "test":
        aln_res = extract_roi_coords_test(record=fastq_record, reference=reference, cap0_pos=cap0_pos) 
    else:
        logger.warn("Invalid train_or_test argument. Must be either 'train' or 'test'.")
        return None
    
    # Extract singal data for the ROI
    start_base_idx_in_fasta = aln_res['left_flanking_region_start_fastq_pos']
    end_base_idx_in_fasta = aln_res['right_flanking_region_start_fastq_pos']
    
    roi_data = extract_roi_signal(signal=pod5_data['signal_pa'], 
                                  base_locs_in_signal=base_locs_in_signal, 
                                  fasta=read_fasta, 
                                  experiment_type=pod5_data['experiment_type'], 
                                  start_base_idx_in_fasta=start_base_idx_in_fasta,
                                  end_base_idx_in_fasta=end_base_idx_in_fasta)
    
    # Add additional information to the ROI data
    roi_data['start_base_idx_in_fasta'] = start_base_idx_in_fasta
    roi_data['end_base_idx_in_fasta'] = end_base_idx_in_fasta   
    roi_data['read_id'] = read_id
    
    if plot_signal:
        read_type = 'bad_reads' if start_base_idx_in_fasta is None and end_base_idx_in_fasta is None else 'good_reads'
        count_key = f'{read_type}_count'
        dir_key = f'{read_type}_dir'
        with lock:
            shared_dict[count_key] = shared_dict.get(count_key, 0) + 1
            if shared_dict[count_key] > 100:
                shared_dict[dir_key] = shared_dict.get(dir_key, 0) + 1
                shared_dict[count_key] = 1
                os.makedirs(os.path.join(output_dir, 'plots', read_type, str(shared_dict[dir_key])), exist_ok=True)
            plot_filepath = os.path.join(output_dir, 'plots', read_type, str(shared_dict[dir_key]), f"{read_id}.html")
            # Write the data for this plot
            worker_state['csv_writer'].writerow([read_id, plot_filepath])
            
        with contextlib.redirect_stdout(None):
            _, _, chunked_aln_str, alignment_score = align(query_seq=read_fasta, target_seq=reference, pretty_print_alns=True)
        plot_roi_signal(pod5_data, bam_data, roi_data, start_base_idx_in_fasta, end_base_idx_in_fasta, plot_filepath, chunked_aln_str, alignment_score)  
    return None
def collate_bam_pod5(bam_filepath, database_path, num_processes, reference, cap0_pos, train_or_test, plot_signal, output_dir):
    num_bam_records = get_total_records(bam_filepath)
    csv_filepath = None  # Initialize the variable here

    if plot_signal:
        good_reads_plots_dir = os.path.join(output_dir, 'plots', 'good_reads', '0')
        bad_reads_plots_dir = os.path.join(output_dir, 'plots', 'bad_reads', '0')
        # create the directories if they do not exist
        os.makedirs(good_reads_plots_dir, exist_ok=True)
        os.makedirs(bad_reads_plots_dir, exist_ok=True)
        # Define the path to the CSV file within the "plots" directory
        csv_filepath = os.path.join(output_dir, 'plots', "plotpaths.csv")
            
    db_handler = DatabaseHandler(database_path, csv_filepath)
    
    with WorkerPool(n_jobs=num_processes, use_worker_state=True) as pool:
        results = pool.map(collate_bam_pod5_worker, 
                           [(bam_data, reference, cap0_pos, train_or_test, plot_signal, output_dir) for bam_data in process_bam_records(bam_filepath)], 
                           iterable_len=num_bam_records,
                           worker_init=db_handler.init_func,  # Passing method of db_handler object
                           worker_exit=db_handler.exit_func,  # Passing method of db_handler object
                           progress_bar=True)

    return results

if __name__ == "__main__":
    bam_filepath = "/export/valenfs/data/processed_data/MinION/9_madcap/1_data/3_20230829_randomcap01/1_basecall/calls.bam"
    database_path =  "/export/valenfs/data/processed_data/MinION/9_madcap/1_data/3_20230829_randomcap01/database.db"
    num_processes = 100
    reference = "CCGGACTTATCGCACCACCTATCCATCATCAGTACTGTNNNNNNCCTGGTAACTGGGAC"
    cap0_pos = 38
    train_or_test = "train"
    output_dir = "/export/valenfs/data/processed_data/MinION/9_madcap/1_data/3_20230829_randomcap01/visualizations"
    plot_signal = True
    # Start profiling
    yappi.set_clock_type("wall")
    yappi.start()

    data = collate_bam_pod5(bam_filepath, database_path, num_processes, reference, cap0_pos, train_or_test, plot_signal, output_dir)

    # Stop profiling
    yappi.stop()

    # Save profiling data in callgrind format
    yappi_stats = yappi.get_func_stats()
    yappi_stats.save('true_output.callgrind', type='callgrind')