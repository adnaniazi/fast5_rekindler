"""
Yields records from a BAM file using pysam
"""

import pysam
import os
from tqdm import tqdm
def generate_bam_records(bam_filepath: str) -> pysam.AlignedSegment:
    """Yield each record from a BAM file. Also creates an index (.bai)
    file if one does not exist already.
    
    Params:
        bam_filepath: str
            Path to the BAM file.
            
    Yields:
        record: pysam.AlignedSegment
            A BAM record.
    """
    index_filepath = f"{bam_filepath}.bai"

    if not os.path.exists(index_filepath):
        pysam.index(bam_filepath)

    with pysam.AlignmentFile(bam_filepath, "rb") as bam_file:
        for record in bam_file:
            yield record
def get_total_records(bam_filepath: str) -> int:
    """Returns the total number of records in a BAM file.
    
    Params:
        bam_filepath: str
            Path to the BAM file.
            
    Returns:
        total_records: int
            Total number of records in the BAM file.
    """        
    bam_file = pysam.AlignmentFile(bam_filepath)
    total_records =  sum(1 for _ in bam_file)
    bam_file.close()
    return total_records
            
def get_signal_info(record: pysam.AlignedSegment) -> dict:
    """Returns the signal info from a BAM record.
    
    Params:
        record: pysam.AlignedSegment
            A BAM record.
            
    Returns:
        signal_info: dict
            Dictionary containing signal info for a read.
    """
    signal_info = dict()
    tags_dict = dict(record.tags)
    moves_table = tags_dict['mv']
    moves_step = moves_table.pop(0)
    signal_info["moves_table"] = moves_table
    signal_info["moves_step"] = moves_step
    signal_info["read_id"] = record.query_name
    signal_info["start_sample"] = tags_dict['ts']
    signal_info["num_samples"] = tags_dict['ns']
    signal_info["quality_score"] = tags_dict['qs']
    signal_info["channel"] = tags_dict['ch']
    signal_info["signal_mean"] = tags_dict['sm']
    signal_info["signal_sd"] = tags_dict['sd']
    signal_info["is_qcfail"] = record.is_qcfail
    signal_info["is_reverse"] = record.is_reverse
    signal_info["is_forward"] = record.is_forward
    signal_info["is_mapped"] = record.is_mapped
    signal_info["is_supplementary"] =  record.is_supplementary
    signal_info["is_secondary"] =  record.is_secondary
    signal_info["read_quality"] = record.qual
    signal_info["read_fasta"] = record.query_sequence
    signal_info["mapping_quality"] = record.mapping_quality
    return signal_info

def process_bam_records(bam_filepath: str) -> dict:
    """Top level function to process a BAM file. 
    Yields signal info for each read in the BAM file.
    
    Params:
        bam_filepath: str
            Path to the BAM file to process.
            
    Yields:
        signal_info: dict
            Dictionary containing signal info for a read.
    """
    for record in generate_bam_records(bam_filepath):
        yield get_signal_info(record)
        

if __name__ == "__main__":
    bam_filepath = "/export/valenfs/data/processed_data/MinION/9_madcap/1_data/3_20230829_randomcap01/1_basecall/calls.bam"
    for read_info in process_bam_records(bam_filepath):
        print(read_info)