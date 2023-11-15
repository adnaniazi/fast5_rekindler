import os
import sqlite3
from pathlib import Path
from typing import Any, Iterator, Tuple

import pod5 as p5  # type: ignore
from mpire import WorkerPool  # type: ignore
from pod5.tools.pod5_convert_to_fast5 import convert_pod5_to_fast5  # type: ignore


def get_pod5_filepath(read_id: str, db_cursor: sqlite3.Cursor) -> Any:
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


def make_pod5_file_list(
    input_dir: str, output_dir: str
) -> Tuple[Iterator[Tuple[str, str]], int]:
    """Recursively find all pod5 files (*.pod5) in input_dir and return a list of Path objects.

    Params:
        input_dir (str): Path to the directory containing the pod5 files.

    Returns:
        pod5_file_list (Tuple[Iterator[Tuple[str, str]], int]): A tuple of zipped pod5 and fast5 filepaths and total number of pod5 files.
    """

    pod5_file_list = []
    fast5_file_list = []
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".pod5"):
                # Construct the full path of the .fast5 file
                pod5_output_path = os.path.join(root, file)

                # Construct the corresponding .pod5 file path in the output directory
                relative_path = os.path.relpath(root, input_dir)
                fast5_output_path = os.path.join(
                    output_dir, relative_path, file.replace(".pod5", ".fast5")
                )

                # Create the subdirectory in the output directory if it doesn't exist
                os.makedirs(os.path.dirname(fast5_output_path), exist_ok=True)

                # Add the paths to the lists
                pod5_file_list.append(pod5_output_path)
                fast5_file_list.append(fast5_output_path)
    return zip(pod5_file_list, fast5_file_list), len(pod5_file_list)


def convert_pod5_to_raw_fast5_worker(pod5_filepath: str, fast5_filepath: str) -> None:
    """Converts a single pod5 file to a raw fast5 file.

    Params:
        pod5_filepath (str): Path to the pod5 file.
        fast5_filepath (str): Path to the output fast5 file.

    Returns:
        None
    """
    # Fetch all the read ids in the POD5 file
    with p5.Reader(pod5_filepath) as reader:
        read_ids = reader.read_ids
    # Convert
    convert_pod5_to_fast5(Path(pod5_filepath), Path(fast5_filepath), read_ids)


def convert_pod5_to_raw_fast5(
    input_dir: str, output_dir: str, num_processes: int = 4
) -> None:
    """
    Converts all pod5 files in input_dir to raw fast5 files and saves them in output_dir.

    Params:
        input_dir (str): Path to the directory containing the pod5 files.
        output_dir (str): Path to the output directory.
        num_processes (int): Number of processes to use. Default is 4.

    Returns:
        None
    """
    p5_f5_filepaths, num_pod5_files = make_pod5_file_list(input_dir, output_dir)

    with WorkerPool(n_jobs=min(num_processes, num_pod5_files)) as pool:
        pool.map(
            convert_pod5_to_raw_fast5_worker,
            [
                (pod5_filepath, fast5_filepath)
                for pod5_filepath, fast5_filepath in p5_f5_filepaths
            ],
            iterable_len=num_pod5_files,
            progress_bar=True,
            progress_bar_options={"desc": "", "unit": "files", "colour": "green"},
        )


if __name__ == "__main__":
    input_dir = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/0_pod5"
    output_dir = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/1_fast5"
    num_processes = 4
    convert_pod5_to_raw_fast5(input_dir, output_dir, num_processes)
