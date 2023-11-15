import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from loguru import logger
from mpire import WorkerPool  # type: ignore
from ont_fast5_api.multi_fast5 import MultiFast5File  # type: ignore

from fast5_rekindler.bam import build_bam_db
from fast5_rekindler.cleanup import cleanup_fast5
from fast5_rekindler.index import build_index_db
from fast5_rekindler.join_dbs import join_databases
from fast5_rekindler.raw_fast5 import convert_pod5_to_raw_fast5


class DatabaseHandler:
    """Class that handles database connections for each worker.
    Each worker can do READ operations in parallel for fetching
    BAM information for a given read_id.
    """

    def __init__(self, database_path: str) -> None:
        """Initializes the database path.

        Params:
            database_path (str): Path to the database file.
        """
        self.database_path = database_path

    def open_database(self) -> Tuple[sqlite3.Cursor, sqlite3.Connection]:
        """Connect to SQLite database (or create it if it doesn't exist)"""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        return cursor, conn

    def init_func(self, worker_state: Dict[str, Any]) -> None:
        """Initializes the database connection for each worker."""
        worker_state["db_cursor"], worker_state["db_connection"] = self.open_database()

    def exit_func(self, worker_state: Dict[str, Any]) -> None:
        """Closes the database connection for each worker."""
        conn = worker_state["db_connection"]
        cursor = worker_state["db_cursor"]
        cursor.close()
        conn.close()


def make_fast5_files_list(directory: str) -> List[str]:
    """Makes a list of all FAST5 files in a directory tree.

    Params:
        directory (str): Path to the directory containing generated raw FAST5 files.

    Returns:
        List[str]: List of all FAST5 file paths.

    """
    fast5_files = []
    # os.walk generates the file names in a directory tree
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".fast5"):
                # os.path.join constructs a full file path
                full_path = os.path.join(root, file)
                fast5_files.append(full_path)
    return fast5_files


def extract_data_from_db(
    read_id: str, conn: sqlite3.Connection, cursor: sqlite3.Cursor
) -> Optional[Dict[str, Any]]:
    """Extracts information from the database for a given read_id.

    Params:
        read_id (str): Read ID.
        conn (sqlite3.Connection): Database connection.
        cursor (sqlite3.Cursor): Database cursor.

    Returns:
        Optional[Dict[str, Any]]: Dictionary containing BAM record information from the database.

    """
    query = """SELECT
                    read_id,
                    fast5_filepath,
                    block_stride,
                    called_events,
                    mean_qscore,
                    sequence_length,
                    duration_template,
                    first_sample_template,
                    num_events_template,
                    moves_table,
                    read_fasta,
                    read_quality
               FROM fast5_bam_db
               WHERE read_id = ?"""

    # Execute the query with the provided read_id
    cursor.execute(query, (read_id,))

    # Fetch the result
    row = cursor.fetchone()

    # If a row is fetched, convert it to a dictionary
    if row:
        columns = [
            "read_id",
            "fast5_filepath",
            "block_stride",
            "called_events",
            "mean_qscore",
            "sequence_length",
            "duration_template",
            "first_sample_template",
            "num_events_template",
            "moves_table",
            "read_fasta",
            "read_quality",
        ]
        record = dict(zip(columns, row))
        return record
    else:
        return None  # No record found for the given read_id


def collate_bam_fast5_worker(worker_state: Dict[str, Any], fast5_filepath: str) -> None:
    """Worker function that fetches information from BAM file and writes it to a raw
    FAST5 file in order to convert it into a basecalled FAST5 file. Unfortunately, some
    information such as the trace table can never be recreated due to a change in the
    basecaller design.

    Params:
        worker_state (Dict[str, Any]): Dictionary containing the database connection and cursor.
        fast5_filepath (str): Path to the raw FAST5 file.

    Returns:
        None
    """
    # Open the file in read/write mode
    mf5 = MultiFast5File(fast5_filepath, mode="r+")
    read_ids_list = mf5.get_read_ids()

    for read_id in read_ids_list:
        try:
            f5_read = mf5.get_read(read_id)
            data = extract_data_from_db(
                read_id, worker_state["db_connection"], worker_state["db_cursor"]
            )
            if data is None:
                # Handle the case where no data is returned. For example:
                logger.warning("No data found for read_id {}", read_id)
                continue  # Skip to the next iteration

            # Add basecalling information to the FAST5 file
            f5_read.add_analysis(
                component="basecall_1d",
                group_name="Basecall_1D_000",
                attrs={
                    "model_type": "flipflop",
                    "name": "ONT Guppy basecalling software.",
                    "segmentation": "Segmentation_000",
                    "time_stamp": "Unknown",
                    "version": "Unknow",
                },
                config=None,
            )
            f5_read.add_analysis_subgroup(
                group_name="Basecall_1D_000", subgroup_name="BaseCalled_template"
            )
            moves_array = np.frombuffer(data["moves_table"], dtype="uint8")
            f5_read.add_analysis_dataset(
                group_name="Basecall_1D_000/BaseCalled_template",
                dataset_name="Move",
                data=moves_array,
                attrs=None,
            )
            fastq = (
                "@"
                + read_id
                + "\n"
                + data["read_fasta"]
                + "\n"
                + "+"
                + "\n"
                + data["read_quality"]
            )
            f5_read.add_analysis_dataset(
                group_name="Basecall_1D_000/BaseCalled_template",
                dataset_name="Fastq",
                data=fastq,
                attrs=None,
            )
            f5_read.add_analysis_subgroup(
                group_name="Basecall_1D_000/Summary",
                subgroup_name="basecall_1d_summary",
                attrs={
                    "block_stride": data["block_stride"],
                    "called_events": data["called_events"],
                    "mean_qscore": data["mean_qscore"],
                    "scaling_mean_abs_dev": 0,
                    "scaling_median": 0,
                    "sequence_length": data["sequence_length"],
                    "skip_prob": 0,
                    "stay_prob": 0,
                    "step_prob": 0,
                    "strand_score": 0,
                },
            )
            f5_read.add_analysis(
                component="segmentation",
                group_name="Segmentation_000",
                attrs={
                    "name": "ONT Guppy basecalling software.",
                    "time_stamp": "Unknown",
                    "version": "Unknown",
                },
            )
            f5_read.add_analysis_subgroup(
                group_name="Segmentation_000/Summary",
                subgroup_name="segmentation",
                attrs={
                    "adapter_max": 0,
                    "duration_template": data["duration_template"],
                    "first_sample_template": data["first_sample_template"],
                    "has_complement": 0,
                    "has_template": 0,
                    "med_abs_dev_template": 0,
                    "median_template": 0,
                    "num_events_template": data["num_events_template"],
                    "pt_detect_success": 0,
                    "pt_median": 0,
                    "pt_sd": 0,
                },
            )
        except Exception:
            logger.exception(
                "Error while processing read_id {} in file {}", read_id, fast5_filepath
            )
    mf5.close()

    # Delete configuration groups
    cleanup_fast5(fast5_filepath)


def collate_bam_fast5(
    bam_filepath: str, pod5_dir: str, output_dir: str, num_processes: int
) -> None:
    """Main function that combines information from BAM and all POD5 files to create
    legacy basecalled FAST5 files.

    Params:
        bam_filepath (str): Path to the BAM file. This file must contain moves information.
        pod5_dir (str): Path to the directory containing POD5 files.
        output_dir (str): Path to the directory where the output FAST5 files will be saved.
                          N.B.: The POD5 directory structure will be replicated in
                          the output directory.
        num_processes (int): Number of processes to use.

    Returns:
        None
    """
    # 1. Convert POD5 to FAST5
    logger.info("Step 1/4: Converting POD5 to raw FAST5 files")
    convert_pod5_to_raw_fast5(pod5_dir, output_dir, num_processes)

    # 2. Make index database
    logger.info("Step 2/4: Building an index database")
    build_index_db(
        fast5_dir=output_dir, output_dir=output_dir, num_processes=num_processes
    )

    # 3. Make BAM database
    logger.info("Step 3/4: Reading BAM file info to the database")
    build_bam_db(bam_filepath, output_dir, num_processes)

    # 4. Join databases
    database_path = join_databases(output_dir)

    # 5. Make a list of all FAST5 files
    logger.info("Step 4/4: Writing basecalling information to FAST5 files")
    fast5_filepaths_list = make_fast5_files_list(output_dir)

    db_handler = DatabaseHandler(database_path)

    num_fast_files = len(fast5_filepaths_list)
    with WorkerPool(
        n_jobs=min(num_processes, num_fast_files), use_worker_state=True
    ) as pool:
        pool.map(
            collate_bam_fast5_worker,
            fast5_filepaths_list,
            iterable_len=num_fast_files,
            worker_init=db_handler.init_func,  # Passing method of db_handler object
            worker_exit=db_handler.exit_func,  # Passing method of db_handler object
            progress_bar=True,
            progress_bar_options={"desc": "", "unit": "files", "colour": "green"},
        )

    os.remove(db_handler.database_path)
    logger.success("Finished successfully!")


if __name__ == "__main__":
    bam_filepath = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/2_bam_file/calls.bam"
    pod5_dir = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/0_pod5"
    output_dir = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/f5r_output13"
    num_processes = 10
    collate_bam_fast5(bam_filepath, pod5_dir, output_dir, num_processes)
