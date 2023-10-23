"""
We cannot random access a record in a BAM file. We can only iterate through it. That is our starting point.
For each record in BAM file, we need to find the corresponding record in POD5 file. For that we need a 
mapping between POD5 file and read_ids. 

Once we have the index, we can go into the relevant POD5 file and get its signal data and collate signal
information with Move table
"""
import pod5 as p5
import sqlite3
import os
from tqdm import tqdm
from typing import List, Tuple
def get_readids(pod5_filepath: str) -> List[str]:
    """
    Get a list of read_ids from a POD5 file.
    
    Params:
        pod5_filepath (str): Path to a POD5 file.
        
    Returns:
        read_ids (List[str]): List of read_ids.
    """
    pod5_fh = p5.Reader(pod5_filepath)
    return pod5_fh.read_ids
    
def open_database(database_path: str) -> Tuple[sqlite3.Cursor, sqlite3.Connection]:
    """
    Open the database connection based on the database path.
    
    Params:
        database_path (str): Path to the database.
        
    Returns:
        cursor (sqlite3.Cursor): Cursor object for the database.
        conn (sqlite3.Connection): Connection object for the database.
    """
    
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    return cursor, conn
  
def prepare_data(read_ids: List[str], pod5_filepath: str) -> List[Tuple[str, str]]:
    """
    Prepare tuples for read_id and pod_filepath for insertion into database.
    
    Params:
        read_ids (List[str]): List of read_ids.
        pod5_filepath (str): Path to a POD5 file.   
        
    Returns:
        data (List[Tuple[str, str]]): List of tuples of read_id and pod5_filepath.
    """
    return [(read_id, pod5_filepath) for read_id in read_ids]
    
def write_database(data:  List[Tuple[str, str]], cursor: sqlite3.Cursor, conn: sqlite3.Connection):
    """
    Write the index to a database.
    
    Params:
        data (List[Tuple[str, str]]): List of tuples of read_id and pod5_filepath.
        cursor (sqlite3.Cursor): Cursor object for the database.
        conn (sqlite3.Connection): Connection object for the database.
        
    Returns:
        None
    """

    # Create table
    cursor.execute('''CREATE TABLE IF NOT EXISTS data (read_id TEXT PRIMARY KEY, pod5_filepath TEXT)''')

    # Insert data
    cursor.executemany("INSERT or REPLACE INTO data VALUES (?, ?)", data)

    # Commit
    conn.commit()
    
def generate_pod5_file_paths(pod5_path: str) -> str:
    """ Traverse the directory and yield all the pod5 file paths.
    
    Params:
        pod5_path (str): Path to a POD5 file or directory of POD5 files.
        
    Returns:
        pod5_filepath (str): Path to a POD5 file.
    """
    
    if os.path.isdir(pod5_path):
        for root, dirs, files in os.walk(pod5_path):
            for file in files:
                if file.endswith(".pod5"):
                    yield os.path.join(root, file)
    elif os.path.isfile(pod5_path) and pod5_path.endswith(".pod5"):
        yield pod5_path

def find_database_size(database_path) -> int:
    """
    Find the number of records in the database.
    
    Params:
        database_path (str): Path to the database.
        
    Returns:
        size (int): Number of records in the database.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM data")
    size = cursor.fetchone()[0]
    return size

    
def index(pod5_path: str, bam_filepath: str, database_path: str) -> None:
    """
    Builds an index mapping read_ids to POD5 file paths.
    
    Params:
        pod5_path (str): Path to a POD5 file or directory of POD5 files.
        bam_filepath (str): Path to a BAM file.
        database_path (str): Path to a database.
        
    Returns:
        None
    """
    
    cursor, conn = open_database(database_path)
    
    total_files = sum(1 for _ in generate_pod5_file_paths(pod5_path))

    for pod5_filepath in tqdm(generate_pod5_file_paths(pod5_path), total=total_files, desc="Processing POD5 files", unit="file"):
        read_ids = get_readids(pod5_filepath)
        data = prepare_data(read_ids, pod5_filepath)
        write_database(data, cursor, conn)

    conn.close()


    
if __name__ == "__main__":
    bam_file = "/export/valenfs/data/processed_data/MinION/9_madcap/1_data/3_20230829_randomcap01/1_basecall/calls.bam"
    pod5_file = "/export/valenfs/data/raw_data/minion/20230829_randomcap01/20230829_randomcap01/20230829_1511_MN21607_FAW07137_2af67808/"
    database_path =  "/export/valenfs/data/processed_data/MinION/9_madcap/1_data/3_20230829_randomcap01/database.db"
    index(pod5_file, bam_file, database_path)
    print("Total number of reads", find_database_size(database_path))
    pass