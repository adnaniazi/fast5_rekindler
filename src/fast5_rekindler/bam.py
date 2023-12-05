"""
Yields records from a BAM file using pysam
"""

import os
import sqlite3
from typing import Any, Dict, Generator

import pysam
from loguru import logger
from mpire import WorkerPool  # type: ignore

TABLE_INIT_QUERY = """CREATE TABLE IF NOT EXISTS bam_db (
                                            read_id TEXT PRIMARY KEY DEFAULT 'Unknown',
                                            parent_read_id TEXT DEFAULT 'NULL',
                                            block_stride INTEGER DEFAULT 0,
                                            called_events INTEGER DEFAULT 0,
                                            mean_qscore FLOAT DEFAULT 0.0,
                                            sequence_length INTEGER DEFAULT 0,
                                            duration_template INTEGER DEFAULT 0,
                                            first_sample_template INTEGER DEFAULT 0,
                                            num_events_template INTEGER DEFAULT 0,
                                            moves_table BLOB DEFAULT NULL,
                                            read_fasta TEXT DEFAULT 'N/A',
                                            read_quality TEXT DEFAULT 'N/A',
                                            action TEXT DEFAULT 'N/A',
                                            split_point INTEGER DEFAULT 0, -- split point without quotes is a reserved keyword
                                            time_stamp TIMESTAMP DEFAULT NULL
                                            )"""


class DatabaseHandler:
    """Database handler for multiprocessing."""

    def __init__(self, output_dir: str, num_processes: int):
        """Initializes the database handler."""
        self.output_dir = output_dir
        self.num_processes = num_processes

    def init_func(self, worker_id: int, worker_state: Dict[str, Any]) -> None:
        """Initializes the database for each worker."""
        unique_db_path = os.path.join(self.output_dir, f"tmp_worker_{worker_id}.db")
        worker_state["db_connection"] = sqlite3.connect(unique_db_path)
        worker_state["db_cursor"] = worker_state["db_connection"].cursor()
        worker_state["db_cursor"].execute(TABLE_INIT_QUERY)
        worker_state["db_connection"].commit()

    def exit_func(self, worker_id: int, worker_state: Dict[str, Any]) -> None:
        """Closes the database connection for each worker."""
        conn = worker_state["db_connection"]
        cursor = worker_state["db_cursor"]
        conn.commit()
        cursor.close()
        conn.close()

    def merge_databases(self) -> None:
        """Merges the databases from each worker into a single database."""
        db_path = os.path.join(self.output_dir, "bam_db.db")
        main_conn = sqlite3.connect(db_path)
        main_cursor = main_conn.cursor()

        main_cursor.execute(TABLE_INIT_QUERY)

        for i in range(self.num_processes):
            worker_db_path = os.path.join(self.output_dir, f"tmp_worker_{i}.db")
            main_cursor.execute(f"ATTACH DATABASE '{worker_db_path}' AS worker_db")

            main_cursor.execute("BEGIN")

            main_cursor.execute(
                """
                INSERT OR IGNORE INTO bam_db
                SELECT * FROM worker_db.bam_db
            """
            )

            main_cursor.execute("COMMIT")
            main_cursor.execute("DETACH DATABASE worker_db")
            os.remove(worker_db_path)

        main_conn.commit()
        main_conn.close()


def get_total_records(bam_filepath: str) -> int:
    """Returns the total number of records in a BAM file.

    Params:
        bam_filepath (str): Path to the BAM file.

    Returns:
        total_records (int): Total number of records in the BAM file.
    """
    with pysam.AlignmentFile(bam_filepath) as bam_file:
        total_records = sum(1 for _ in bam_file)
    return total_records


def get_signal_info(record: pysam.AlignedSegment) -> Dict[str, Any]:
    """Returns a dictionary containing the signal information from a BAM record.

    Params:
        record (pysam.AlignedSegment): A BAM record.

    Returns:
        signal_info (Dict[str, Any]): A dictionary containing the signal information.
    """
    signal_info = {}
    tags_dict = dict(record.tags)  # type: ignore
    try:
        signal_info["moves_table"] = tags_dict["mv"]
    except KeyError:
        logger.exception(
            "The BAM file does not contain moves information. Please use --emit-moves option when basecalling using Doardo."
        )
        raise SystemExit
    signal_info["moves_step"] = signal_info["moves_table"].pop(0)
    signal_info["read_id"] = record.query_name
    signal_info["start_sample"] = tags_dict["ts"]
    signal_info["num_samples"] = tags_dict["ns"]
    signal_info["quality_score"] = tags_dict["qs"]
    signal_info["channel"] = tags_dict["ch"]
    signal_info["signal_mean"] = tags_dict["sm"]
    signal_info["signal_sd"] = tags_dict["sd"]
    signal_info["is_qcfail"] = record.is_qcfail
    signal_info["is_reverse"] = record.is_reverse
    signal_info["is_forward"] = record.is_forward  # type: ignore
    signal_info["is_mapped"] = record.is_mapped  # type: ignore
    signal_info["is_supplementary"] = record.is_supplementary
    signal_info["is_secondary"] = record.is_secondary
    signal_info["read_quality"] = record.qual  # type: ignore
    signal_info["read_fasta"] = record.query_sequence
    signal_info["mapping_quality"] = record.mapping_quality
    signal_info["parent_read_id"] = tags_dict.get("pi", "")
    signal_info["split_point"] = tags_dict.get("sp", 0)
    signal_info["time_stamp"] = tags_dict.get("st")

    return signal_info


def process_bam_records(bam_filepath: str) -> Generator[Dict[str, Any], None, None]:
    """Yields records from a BAM file using pysam one-by-one and
    extracts the signal information from each of the record.

    Params:
        bam_filepath (str): Path to the BAM file.

    Yields:
        signal_info Generator[Dict[str, Any], None, None]: A dictionary containing the signal information.

    """
    index_filepath = f"{bam_filepath}.bai"

    if not os.path.exists(index_filepath):
        pysam.index(bam_filepath)  # type: ignore

    with pysam.AlignmentFile(bam_filepath, "rb") as bam_file:
        for record in bam_file:
            yield get_signal_info(record)


def insert_bamdata_in_db_worker(
    worker_id: int, worker_state: Dict[str, Any], bam_data: Dict[str, Any]
) -> None:
    """Inserts the signal information from a BAM record into the BAM database.

    Params:
        worker_id (int): Worker ID.
        worker_state (Dict[str, Any]): Worker state.
        bam_data (Dict[str, Any]): A dictionary containing the signal information.

    Returns:
        None
    """
    cursor = worker_state["db_cursor"]
    if bam_data.get("is_secondary"):
        # Skip supplementary alignments as they do not contain FASTA information
        # The FASTA information is only present in the primary alignment
        # and is picked up from these record of the primary alignment
        return

    try:
        # Extract the required attributes from the BAM data dictionary
        block_stride = bam_data.get("moves_step")
        moves_table = bam_data.get("moves_table")
        called_events = len(moves_table) if moves_table is not None else 0
        mean_qscore = bam_data.get("quality_score")
        read_fasta = bam_data.get("read_fasta")
        sequence_length = len(read_fasta) if read_fasta is not None else 0
        duration_template = bam_data.get("num_samples")
        first_sample_template = bam_data.get("start_sample")
        num_events_template = called_events
        moves_table = bam_data.get("moves_table").tobytes()  # type: ignore
        read_fasta = bam_data.get("read_fasta")
        read_quality = bam_data.get("read_quality")
        read_id = bam_data.get("read_id")
        parent_read_id = bam_data.get("parent_read_id")
        split_point = bam_data.get("split_point")
        time_stamp = bam_data.get("time_stamp")

        insert_query = """INSERT OR IGNORE INTO bam_db (
                            block_stride,
                            called_events,
                            mean_qscore,
                            sequence_length,
                            duration_template,
                            first_sample_template,
                            num_events_template,
                            moves_table,
                            read_fasta,
                            read_quality,
                            read_id,
                            parent_read_id,
                            split_point,
                            time_stamp
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        cursor.execute(
            insert_query,
            (
                block_stride,
                called_events,
                mean_qscore,
                sequence_length,
                duration_template,
                first_sample_template,
                num_events_template,
                moves_table,
                read_fasta,
                read_quality,
                read_id,
                parent_read_id,
                split_point,
                time_stamp,
            ),
        )
    except Exception:
        logger.exception(
            "An error occurred in worker {}, read_id: {}", worker_id, read_id
        )


def build_bam_db(bam_filepath: str, output_dir: str, num_processes: int) -> None:
    """Builds a database from a BAM file.

    Params:
        bam_filepath (str): Path to the BAM file.
        output_dir (str): Path to the output directory.
        num_processes (int): Number of processes to use.

    Returns:
        None
    """
    num_bam_records = get_total_records(bam_filepath)
    db_handler = DatabaseHandler(output_dir, num_processes)
    with WorkerPool(
        n_jobs=num_processes, use_worker_state=True, pass_worker_id=True
    ) as pool:
        pool.map(
            insert_bamdata_in_db_worker,
            [(bam_data,) for bam_data in process_bam_records(bam_filepath)],
            iterable_len=num_bam_records,
            worker_init=db_handler.init_func,
            worker_exit=db_handler.exit_func,
            progress_bar=True,
            progress_bar_options={"desc": "", "unit": "records", "colour": "green"},
        )
    db_handler.merge_databases()


if __name__ == "__main__":
    bam_filepath = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/2_bam_file/calls.bam"
    output_dir = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/f5r_output7/"
    num_processes = 10
    build_bam_db(bam_filepath, output_dir, num_processes)
