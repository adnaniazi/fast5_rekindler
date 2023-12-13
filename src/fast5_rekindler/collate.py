import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple, Union

import h5py  # type: ignore
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


def extract_read_data_from_db_based_on_action(
    fast5_filepath: str, action: str, worker_state: Dict[str, Any]
) -> Optional[List[Dict[str, Any]]]:
    """
    Extracts information from the database for a given FAST5 file
    based on a given action.

    Params:
        fast5_filepath (str): Path to the raw FAST5 file.
        action (str): Action to query ('duplicate' or 'delete').
        worker_state (Dict[str, Any]): Dictionary containing the database connection and cursor.

    Returns:
        Optional[List[Dict[str, Any]]]: List of dictionaries containing BAM record information from the database.
    """
    cursor = worker_state.get("db_cursor")
    assert cursor is not None, "Database cursor is unexpectedly None"

    query = """SELECT
                    read_id,
                    parent_read_id,
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
                    read_quality,
                    action,
                    split_point
                FROM fast5_bam_db
                WHERE fast5_filepath = ? AND action = ?
            """

    # Execute the query with the provided ID value
    cursor.execute(query, (fast5_filepath, action))

    # Define the columns
    columns = [
        "read_id",
        "parent_read_id",
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
        "action",
        "split_point",
    ]

    rows = cursor.fetchall()

    if not rows:
        return None

    return [dict(zip(columns, row)) for row in rows]


def extract_simplex_data_from_db(
    id_value: str,
    id_type: str,
    worker_state: Dict[str, Any],
    fetch_multiple: bool = False,
) -> Union[Dict[str, Any], List[Dict[str, Any]], None]:
    """
    Extracts information from the database for a given read ID or parent read ID.

    Params:
        id_value (str): Value of the read ID or parent read ID.
        id_type (str): Type of ID to query ('read_id' or 'parent_read_id').
        worker_state (Dict[str, Any]): Dictionary containing the database connection and cursor.
        fetch_multiple (bool): Flag to fetch multiple records or a single record.

    Returns:
        Union[Optional[Dict[str, Any]], Generator[Dict[str, Any], None, None]]:
        - Dictionary containing BAM record information from the database, or None if no record is found.
        - A generator that yields dictionaries containing BAM record information from the database.
    """
    cursor = worker_state.get("db_cursor")
    assert cursor is not None, "Database cursor is unexpectedly None"

    query = f"""SELECT
                    read_id,
                    parent_read_id,
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
                    read_quality,
                    action,
                    time_stamp
                FROM fast5_bam_db
                WHERE {id_type} = ?"""

    # Execute the query with the provided ID value
    cursor.execute(query, (id_value,))

    # Define the columns
    columns = [
        "read_id",
        "parent_read_id",
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
        "action",
        "time_stamp",
    ]

    if fetch_multiple:
        # Fetch and yield the results one by one
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    else:
        # Fetch the first result
        row = cursor.fetchone()

        # If a row is fetched, convert it to a dictionary
        if row:
            return dict(zip(columns, row))
        else:
            return None  # No record found for the given ID


def copy_group(source_group: Any, target_group: Any) -> None:
    """Recursively copies a group and its contents to a target group in an HDF5 file.

    Params:
        source_group (Any): HDF5 source group.
        target_group (Any): HDF5 arget group.

    Returns:
        None
    """
    # Copy attributes from the source group to the target group
    for attr_name, attr_value in source_group.attrs.items():
        if attr_name == "read_id":
            attr_value = target_group.name.split("_")[-1]
        target_group.attrs[attr_name] = attr_value

    # Recursively copy subgroups and datasets from the source group to the target group
    for name, obj in source_group.items():
        if isinstance(obj, h5py.Group):
            new_subgroup = target_group.create_group(name)
            copy_group(obj, new_subgroup)
        elif isinstance(obj, h5py.Dataset):
            # Copy the dataset to the target group
            source_group.copy(obj, target_group, name=name)


def duplicate_and_rename_group(
    input_file: str, original_group_name: str, new_group_name: str
) -> None:
    """Duplicates a read group in an HDF5 file and renames it.

    Params:
        input_file (str): Path to the HDF5 file.
        original_group_name (str): Name of the original group.
        new_group_name (str): Name of the new group.

    Returns:
        None
    """
    with h5py.File(input_file, "r+") as file:
        # Check if the original group exists
        if original_group_name not in file:
            print(f"The group '{original_group_name}' does not exist in the HDF5 file.")
            return

        # Read the original group
        original_group = file[original_group_name]

        # Create a new group with the desired new name
        new_group = file.create_group(new_group_name)

        # Copy contents of the original group to the new group
        copy_group(original_group, new_group)


def create_duplex_reads_in_fast5(
    worker_state: Dict[str, Any], fast5_filepath: str
) -> None:
    """Creates extra reads for duplex/chimeric reads that are split into two reads in the BAM file.
    It copies all the information in the parent read group and renames it to the child read group.

    Params:
        worker_state (Dict[str, Any]): Dictionary containing the database connection and cursor.
        fast5_filepath (str): Path to the raw FAST5 file.

    Returns:
        None
    """
    data = extract_read_data_from_db_based_on_action(
        fast5_filepath, action="duplicate", worker_state=worker_state
    )

    if data is None:
        return

    for record in data:
        read_id = record.get("read_id")
        parent_read_id = record.get("parent_read_id")
        original_group_name = f"/read_{parent_read_id}"
        new_group_name = f"/read_{read_id}"
        duplicate_and_rename_group(fast5_filepath, original_group_name, new_group_name)


def delete_nonexistant_reads_from_fast5(
    worker_state: Dict[str, Any], fast5_filepath: str
) -> None:
    """Deletes reads that are:
        a. Not in the BAM file but have no corresponding record in FAST5 file
        b. Duplex parent reads for which with no corresponding record in BAM file

    Params:
        worker_state (Dict[str, Any]): Dictionary containing the database connection and cursor.
        fast5_filepath (str): Path to the raw FAST5 file.

    Returns:
        None
    """
    data_del = extract_read_data_from_db_based_on_action(
        fast5_filepath, action="delete", worker_state=worker_state
    )

    # Check if combined_data is still empty
    if not data_del:
        return

    with h5py.File(fast5_filepath, "r+") as file:
        for record in data_del:
            read_id = record.get("read_id")
            group_name = f"/read_{read_id}"
            # Use the `del` statement to delete the group
            del file[group_name]


def split_hdf5_signal_array(
    fileobj: Any, dataset_name: str, start_idx: int, end_idx: int
) -> None:
    """
    Splits the raw signal array of a read into two parts and stores them in a new dataset.

    Params:
        fileobj (Any): File object of the HDF5 file.
        dataset_name (str): Name of the dataset containing the raw signal array.
        start_idx (int): Start index of the split.
        end_idx (int): End index of the split.

    Returns:
        None
    """
    # Read the original dataset
    original_dataset = fileobj[dataset_name]

    # Perform the trim operation based on start and end indices
    trimmed_data = original_dataset[start_idx:end_idx]  # +1 to include the end index

    # Delete the original dataset
    del fileobj[dataset_name]

    # Create a new dataset with the same name and store the trimmed data
    fileobj.create_dataset(dataset_name, data=trimmed_data)


def split_raw_signal_for_duplex_reads(
    worker_state: Dict[str, Any], fast5_filepath: str
) -> None:
    """Splits the raw signal of a duplex read into its child reads.

    Params:
        worker_state (Dict[str, Any]): Dictionary containing the database connection and cursor.
        fast5_filepath (str): Path to the raw FAST5 file.

    Returns:
        None

    """
    data = extract_read_data_from_db_based_on_action(
        fast5_filepath, action="duplicate", worker_state=worker_state
    )
    if data is None:
        return
    with h5py.File(fast5_filepath, "r+") as fileobj:
        for record in data:
            read_id = record.get("read_id")
            dataset_name = f"/read_{read_id}/Raw/Signal"
            split_point = record.get("split_point")
            first_sample_template = record.get("first_sample_template")
            if first_sample_template is None or split_point is None:
                logger.error(
                    "Missing first_sample_template or split_point for read_id: {}",
                    read_id,
                )
                continue
            first_sample_template += split_point

            duration = record.get("duration_template")
            if duration > first_sample_template:
                # This is the first of the chimeric read's child
                end_idx = duration
            else:
                # This is the second of the chimeric read's child
                end_idx = first_sample_template + duration
            # assert that end_idx is integer and greater than first_sample_template
            assert isinstance(end_idx, int) and end_idx > first_sample_template
            split_hdf5_signal_array(
                fileobj, dataset_name, start_idx=first_sample_template, end_idx=end_idx
            )


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
    # First create extra reads for duplex/chimeric read that are split
    # into two reads in the BAM file
    create_duplex_reads_in_fast5(worker_state, fast5_filepath)

    # Delete reads that are not in the BAM file
    delete_nonexistant_reads_from_fast5(worker_state, fast5_filepath)

    # Handle all the simplex/unfused reads
    mf5 = MultiFast5File(fast5_filepath, mode="r+")
    read_ids_list = mf5.get_read_ids()

    for read_id in read_ids_list:
        f5_read = mf5.get_read(read_id)
        try:
            data = extract_simplex_data_from_db(
                read_id,
                "read_id",
                worker_state=worker_state,
                fetch_multiple=False,
            )
            if isinstance(data, dict):
                add_info_to_fast5_read(f5_read, data, read_id)
            else:
                logger.error("Data for read_id {} is not in expected format", read_id)
        except Exception:
            logger.debug("Error in read_id {} in file {}.", read_id, fast5_filepath)
    mf5.close()

    # Delete configuration groups
    cleanup_fast5(fast5_filepath)

    # For duplex/chimeric reads, split the original raw signal properly between the two reads
    # The splitted raw signal is not VBZ compressed
    split_raw_signal_for_duplex_reads(worker_state, fast5_filepath)


def add_info_to_fast5_read(f5_read: Any, data: Dict[str, Any], read_id: str) -> None:
    """Adds basecalling information the raw FAST5 read.

    Params:
        f5_read (Any): Raw FAST5 read object.
        data (Dict[str, Any]): Dictionary containing basecalling information.
        read_id (str): Read ID.

    Returns:
        None
    """
    f5_read.add_analysis(
        component="basecall_1d",
        group_name="Basecall_1D_000",
        attrs={
            "model_type": "flipflop",
            "name": "ONT Guppy basecalling software.",
            "segmentation": "Segmentation_000",
            "time_stamp": data.get("time_stamp"),
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
        subgroup_name="basecall_1d_template",
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
            "parent_read_id": data["parent_read_id"],
        },
    )


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
    logger.info("Step 1/4: Converting POD5 files to raw FAST5 files")
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
    output_dir = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/f5r_output14"
    num_processes = 10
    collate_bam_fast5(bam_filepath, pod5_dir, output_dir, num_processes)
