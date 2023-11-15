import os
import sqlite3


def join_databases(output_dir: str) -> str:
    """Merges the index_db and bam_db databases into a
    single output database fast5_bam_db.db.

    Params:
        output_dir (str): Path to the output directory.

    Returns:
        output_db_path (str): Path to the output database.
    """
    bam_db_path = os.path.join(output_dir, "bam_db.db")
    index_db_path = os.path.join(output_dir, "index_db.db")
    output_db_path = os.path.join(output_dir, "fast5_bam_db.db")

    if os.path.exists(output_db_path):
        os.remove(output_db_path)
    # Create output database connection
    conn_output = sqlite3.connect(output_db_path)
    cursor_output = conn_output.cursor()

    # Create the table in the output database
    cursor_output.execute(
        """CREATE TABLE IF NOT EXISTS fast5_bam_db (
                            read_id TEXT PRIMARY KEY,
                            fast5_filepath TEXT,
                            block_stride INTEGER,
                            called_events INTEGER,
                            mean_qscore FLOAT,
                            sequence_length INTEGER,
                            duration_template INTEGER,
                            first_sample_template INTEGER,
                            num_events_template INTEGER,
                            moves_table BLOB,
                            read_fasta TEXT,
                            read_quality TEXT
                        )"""
    )

    # Attach both databases to the new database connection
    cursor_output.execute(f"ATTACH DATABASE '{index_db_path}' AS idx")
    cursor_output.execute(f"ATTACH DATABASE '{bam_db_path}' AS bam")

    # Perform the JOIN operation and insert the data into the new database
    cursor_output.execute(
        """INSERT INTO fast5_bam_db
                                SELECT b.read_id,
                                       i.fast5_filepath,
                                       b.block_stride,
                                       b.called_events,
                                       b.mean_qscore,
                                       b.sequence_length,
                                       b.duration_template,
                                       b.first_sample_template,
                                       b.num_events_template,
                                       b.moves_table,
                                       b.read_fasta,
                                       b.read_quality
                                FROM bam.bam_db AS b
                                INNER JOIN idx.index_db AS i ON b.read_id = i.read_id"""
    )

    # Commit the changes
    conn_output.commit()

    # Check if the data has been inserted
    cursor_output.execute("SELECT COUNT(*) FROM fast5_bam_db")
    row_count = cursor_output.fetchone()[0]
    if row_count > 0:
        # If data is inserted successfully, delete the original databases
        os.remove(index_db_path)
        os.remove(bam_db_path)
    else:
        print("No data was inserted. Original databases have not been removed.")

    # Close the connection
    cursor_output.close()
    conn_output.close()

    return output_db_path


if __name__ == "__main__":
    # Example usage:
    output_dir = "/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/f5r_output/"
    index_db_path = os.path.join(output_dir, "index_db.db")
    bam_db_path = os.path.join(output_dir, "bam_db.db")
    join_databases(output_dir)
