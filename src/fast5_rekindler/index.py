import os
import sqlite3
from typing import Any, Dict, Generator, List, Tuple

from mpire import WorkerPool  # type: ignore
from ont_fast5_api.fast5_interface import get_fast5_file  # type: ignore

# Constants
TABLE_INIT_QUERY = """CREATE TABLE IF NOT EXISTS index_db (
    read_id TEXT PRIMARY KEY DEFAULT 'Unknown',
    fast5_filepath TEXT DEFAULT 'N/A'
)"""


class DatabaseHandler:
    """Database handler for multiprocessing."""

    def __init__(self, output_dir: str, num_processes: int) -> None:
        self.output_dir = output_dir
        self.num_processes = num_processes

    def init_func(self, worker_id: int, worker_state: Dict[str, Any]) -> None:
        """Initializes the database connection for each worker."""
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
        db_path = os.path.join(self.output_dir, "index_db.db")
        main_conn = sqlite3.connect(db_path)
        main_cursor = main_conn.cursor()
        main_cursor.execute(TABLE_INIT_QUERY)
        for i in range(self.num_processes):
            worker_db_path = os.path.join(self.output_dir, f"tmp_worker_{i}.db")
            main_cursor.execute(f"ATTACH DATABASE '{worker_db_path}' AS worker_db")
            main_cursor.execute("BEGIN")
            main_cursor.execute(
                """
                INSERT OR IGNORE INTO index_db
                SELECT * FROM worker_db.index_db
            """
            )
            main_cursor.execute("COMMIT")
            main_cursor.execute("DETACH DATABASE worker_db")
            os.remove(worker_db_path)
        main_conn.commit()
        main_conn.close()


def get_readids(fast5_filepath: str) -> List[str]:
    """
    Get a list of read_ids from a FAST5 file.

    Params:
        fast5_filepath (str): Path to a FAST5 file.

    Returns:
        read_ids (List[str]): List of read_ids.
    """
    read_ids = []
    with get_fast5_file(fast5_filepath, mode="r") as f5:
        for read in f5.get_reads():
            read_ids.append(read.read_id)
    return read_ids


def prepare_data(read_ids: List[str], fast5_filepath: str) -> List[Tuple[str, str]]:
    """
    Prepare tuples for read_id and fast5_filepath for insertion into database.

    Params:
        read_ids (List[str]): List of read_ids.
        fast5_filepath (str): Path to a FAST5 file.

    Returns:
        data (List[Tuple[str, str]]): List of tuples of read_id, fast5_filepath
        and other relevant read data.
    """
    return [(read_id, fast5_filepath) for read_id in read_ids]


def write_database(
    data: List[Tuple[str, str]], cursor: sqlite3.Cursor, conn: sqlite3.Connection
) -> None:
    """
    Write the index data (read_id, filepath) to the database.

    Params:
        data (List[Tuple[str, str, int, int, float, int, int, int, int]]): List of tuples read_id and filepath
        cursor (sqlite3.Cursor): Cursor object for the database.
        conn (sqlite3.Connection): Connection object for the database.

    Returns:
        None
    """
    cursor.executemany(
        "INSERT OR IGNORE INTO index_db (read_id, fast5_filepath) VALUES (?, ?)", data
    )


def generate_fast5_file_paths(fast5_dir: str) -> Generator:
    """Traverse the directory and yield all the fast5 file paths.

    Params:
        fast5_dir (str): Path to a FAST5 file or directory of FAST5 files.

    Returns:
        pod5_filepath (str): Path to a FAST5 file.
    """

    if os.path.isdir(fast5_dir):
        for root, _, files in os.walk(fast5_dir):
            for file in files:
                if file.endswith(".fast5"):
                    yield os.path.join(root, file)
    elif os.path.isfile(fast5_dir) and fast5_dir.endswith(".fast5"):
        yield fast5_dir


def find_database_size(output_dir: str) -> Any:
    """
    Find the number of records in the database.

    Params:
        output_dir (str): Path to the database directory.

    Returns:
        size (Any): Number of records in the database.
    """
    database_path = os.path.join(output_dir, "index_db.db")
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM index_db")
    size = cursor.fetchone()[0]
    return size


def build_index_db_worker(
    worker_id: int, worker_state: Dict[str, Any], fast5_filepath: str
) -> None:
    """
    Builds an index mapping read_ids to FAST5 file paths. Every worker has its own database.

    Params:
        fast5_filepath (str): Path to a FAST5 file.
        worker_id (int): Worker ID.
        worker_state (Dict[str, Any]): Worker state dictionary.

    Returns:
        None
    """
    read_ids = get_readids(fast5_filepath)
    data = prepare_data(read_ids, fast5_filepath)
    write_database(data, worker_state["db_cursor"], worker_state["db_connection"])


def build_index_db(fast5_dir: str, output_dir: str, num_processes: int) -> None:
    """
    Builds an index mapping read_ids to FAST5 file paths.

    Params:
        fast5_dir (str): Path to a FAST5 file or directory of FAST5 files.
        output_dir (str): Path to a output directory.
        num_processes (int): Number of processes to use.

    Returns:
        None
    """
    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    total_files = sum(1 for _ in generate_fast5_file_paths(fast5_dir))

    num_processes = min(num_processes, total_files)
    db_handler = DatabaseHandler(output_dir, num_processes)

    with WorkerPool(
        n_jobs=num_processes, use_worker_state=True, pass_worker_id=True
    ) as pool:
        pool.map(
            build_index_db_worker,
            [
                (fast5_filepath,)
                for fast5_filepath in generate_fast5_file_paths(fast5_dir)
            ],
            iterable_len=total_files,
            worker_init=db_handler.init_func,
            worker_exit=db_handler.exit_func,
            progress_bar=True,
            progress_bar_options={"desc": "", "unit": "files", "colour": "green"},
        )
    db_handler.merge_databases()


if __name__ == "__main__":
    fast5_dir = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/f5r_output7"
    output_dir = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/f5r_output7"
    num_processes = 5
    build_index_db(fast5_dir, output_dir, num_processes)
    print("Total number of reads", find_database_size(output_dir))
