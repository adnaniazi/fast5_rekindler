"""
Given read_id and pod5 filepath, this file prepares signal data for downstream usage
"""

import pod5 as p5
import polars as pl
import os
from capfinder.bam import process_bam_records
import sqlite3
from typing import Tuple
import numpy as np
import matplotlib.pyplot as plt
from logger_config import logger
from mpire import WorkerPool  # type: ignore

def open_database(database_path: str) -> Tuple[sqlite3.Cursor, sqlite3.Connection]:
    """
    Open the database connection based on the database path.
    
    Params:
        database_path (str): Path to the database.
        
    Returns:
        cursor (sqlite3.Cursor): Cursor object for the database.
        conn (sqlite3.Connection): Connection object for the database.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    return conn, cursor


def get_pod5_filepath(read_id, db_cursor):
    """Fetches the pod5 filepath for a given read_id 
    from the SQLite index database.
    
    Params:
        read_id: str
            The read_id of the read to be extracted.
        db_cursor: sqlite3.Cursor
            Cursor object for the database.
    Returns:
        pod5_filepath: str
            Path to the pod5 file.
    """
    db_cursor.execute("SELECT pod5_filepath FROM data WHERE read_id = ?", (read_id,))
    result = db_cursor.fetchone()
    return result[0] if result else None

def pull_read_from_pod5(read_id:str, pod5_filepath:str):
    """Returns a single read from a pod5 file.
    
    Params:
        read_id: str
            The read_id of the read to be extracted.
        pod5_filepath: str
            Path to the pod5 file.
            
    Returns:
        read: pod5.Read
            A pod5 Read object.
    """
    signal_dict = dict()
    with p5.Reader(pod5_filepath) as reader:

        # Read the selected read from the pod5 file
        read = next(reader.reads(selection=[read_id]))

        # Get the signal data and sample rate
        signal_dict['sample_rate'] = read.run_info.sample_rate
        signal_dict['sequencing_kit'] = read.run_info.sequencing_kit
        signal_dict['experiment_type'] = read.run_info.context_tags['experiment_type']
        signal_dict['local_basecalling'] = read.run_info.context_tags['local_basecalling']
        signal_dict['signal'] = read.signal
        signal_dict['signal_pa'] = read.signal_pa
        signal_dict['end_reason'] = read.end_reason.reason.name
        signal_dict['sample_count'] = read.sample_count
        signal_dict['channel'] = read.pore.channel
        signal_dict['well'] = read.pore.well
        signal_dict['pore_type'] = read.pore.pore_type
        signal_dict['writing_software'] = reader.writing_software    
        signal_dict['scale'] = read.tracked_scaling.scale
        signal_dict['shift'] = read.tracked_scaling.shift
    return signal_dict

def find_base_locs_in_signal(bam_data: dict) -> np.ndarray:
    """
    Finds the locations of each new base in the signal.
    
    Params:
        bam_data (dict): Dictionary containing information from the BAM file.
        
    Returns:
        base_locs_in_signal (np.ndarray): Array of locations of each new base in the signal.
    """
    start_sample = bam_data["start_sample"]
    moves_step = bam_data["moves_step"]
    moves_table = np.array(bam_data["moves_table"])
    
    # Where do moves occur in the signal coordinates?
    moves_indices = np.arange(start_sample, start_sample + moves_step * len(moves_table), moves_step)
    
    # We only need locs where a move of 1 occurs
    base_locs = moves_indices * moves_table
    base_locs_in_signal = base_locs[base_locs != 0]
    
    return base_locs_in_signal

def extract_roi_signal(signal: np.ndarray, 
                       base_locs_in_signal: np.ndarray, 
                       fasta: str, 
                       experiment_type: str, 
                       start_base_idx_in_fasta: int, 
                       end_base_idx_in_fasta: int):
    """
    Extracts the signal data for a region of interest (ROI).
    
    Params:
        signal (np.ndarray): Signal data.
        base_locs_in_signal (np.ndarray): Array of locations of each new base in the signal.
        fasta (str): Fasta sequence of the read.
        experiment_type (str): Type of experiment (rna or dna).
        start_base_idx_in_fasta (int): Index of the first base in the ROI.
        end_base_idx_in_fasta (int): Index of the last base in the ROI.
        
    Returns:
        roi_data (dict): Dictionary containing the ROI signal and fasta sequence.
    """
    roi_data = dict()
    roi_data['roi_fasta'] = None
    roi_data['roi_signal'] = np.array([])
    roi_data['signal_start'] = None
    roi_data['signal_end'] = None
    roi_data['plot_signal'] = None
    roi_data['base_locs_in_signal'] = base_locs_in_signal

    # Check for valid inputs
    if end_base_idx_in_fasta is  None and start_base_idx_in_fasta is None:
        return roi_data

    if end_base_idx_in_fasta > len(fasta) or start_base_idx_in_fasta < 0:
        logger.warn("Invalid start or end base index in fasta.")
        return roi_data
        
    if experiment_type not in ('rna', 'dna'):
        logger.warn("Invalid experiment type.")
        return roi_data

    if experiment_type == 'rna':
        rev_base_locs_in_signal = base_locs_in_signal[::-1]
        signal_end = rev_base_locs_in_signal[start_base_idx_in_fasta-1]
        signal_start = rev_base_locs_in_signal[end_base_idx_in_fasta-1]
        roi_data['roi_fasta'] = fasta[start_base_idx_in_fasta:end_base_idx_in_fasta]
    else:
        signal_start = base_locs_in_signal[start_base_idx_in_fasta-1] # TODO: Confirm -1
        signal_end = base_locs_in_signal[end_base_idx_in_fasta-1]     # TODO: Confirm -1
        roi_data['roi_fasta'] = fasta[start_base_idx_in_fasta:end_base_idx_in_fasta]
        
    # Signal data is 3'-> 5' for RNA 5' -> 3' for DNA 
    # The ROI FASTA is always 5' -> 3' irrespective of the experiment type
    roi_data['roi_signal'] = signal[signal_start:signal_end]
    
    # Make roi signal for plot and pad it with NaNs outside the ROI
    plot_signal = np.copy(signal)
    plot_signal[:signal_start] = np.nan
    plot_signal[signal_end:] = np.nan
    roi_data['signal_start'] = signal_start
    roi_data['signal_end'] = signal_end
    roi_data['roi_signal_for_plot'] = plot_signal
    roi_data['base_locs_in_signal'] = base_locs_in_signal
    
    return roi_data


if __name__ == '__main__':
    bam_filepath = "/export/valenfs/data/processed_data/MinION/9_madcap/1_data/3_20230829_randomcap01/1_basecall/calls.bam"
    database_path =  "/export/valenfs/data/processed_data/MinION/9_madcap/1_data/3_20230829_randomcap01/database.db"
    
    # open index data base connection
    conn, cursor = open_database(database_path)

    for bam_data in process_bam_records(bam_filepath):
        # Get read_id from bam record
        read_id = bam_data["read_id"]
        
        # Find the pod5 filepath for the read_id from index database
        pod5_filepath = get_pod5_filepath(read_id, conn, cursor)
        
        # Pull the read data from the multi-pod5 file
        pod5_data = pull_read_from_pod5(read_id, pod5_filepath)
        
        # Extract the locations of each new base in signal coordinates
        base_locs_in_signal = find_base_locs_in_signal(bam_data)
        
        # Extract singal data for region of interest 
        roi_data = extract_roi_signal(signal=pod5_data['signal'], 
                                      base_locs_in_signal=base_locs_in_signal, 
                                      fasta=bam_data['read_fasta'], 
                                      experiment_type='rna', 
                                      start_base_idx_in_fasta=1,
                                      end_base_idx_in_fasta=10)
        pass

    # num_processes = 4
    # with WorkerPool(n_jobs=num_processes) as pool:
    #     results = pool.map(
    #         process_read,
    #         [(record, reference, cap0_pos) for record in records],
    #         iterable_len=total_records,
    #         progress_bar=True,
    #     )