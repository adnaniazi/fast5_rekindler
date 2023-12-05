import argparse
import sys

from loguru import logger

from fast5_rekindler.collate import (
    collate_bam_fast5,  # Replace with your actual module name
)
from fast5_rekindler.logger_config import configure_logger, get_version


def fast5_rekindler() -> None:
    """CLI entry point for rekindle_fast5 package."""
    app_version = get_version()

    # Use RawTextHelpFormatter to preserve newlines
    parser = argparse.ArgumentParser(
        description="Creates legacy basecalled FAST5 files by collating information in BAM and POD5 files.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""Example usage:
fast5_rekindler /path/to/calls.bam /path/to/pod5_dir /path/to/output_dir -n 4
        """,
    )

    # Positional arguments
    parser.add_argument(
        "bam_filepath",
        type=str,
        help=(
            "Path to the BAM file. \n"
            "This file must contain moves information. \n"
            "To add moves info to the BAM file, \n"
            "use --emit-moves option when basecalling using Doardo."
        ),
    )
    parser.add_argument("pod5_dir", type=str, help="Directory containing POD5 files.")
    parser.add_argument(
        "output_dir",
        type=str,
        help="Directory where the output FAST5 files will be saved.",
    )

    # Optional arguments (options)
    parser.add_argument(
        "-n",
        "--num-processes",
        type=int,
        default=4,
        metavar="",
        help="Number of processes to use. Default is 4.",
    )

    # Version argument
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {app_version}",
        help="Show program's version number and exit",
    )  # Replace 1.0.0 with your actual version

    args = parser.parse_args()

    # Initialize the logger
    log_filepath = configure_logger(new_log_directory=args.output_dir)

    width = 15
    full_command = " ".join(sys.argv)
    logger.info("You have issued the following command: {}\n", full_command)

    logger.info(
        """You have configured the following options:
        {} {}
        {} {}
        {} {}
        {} {}
        {} {}, \n""",
        "• BAM filepath:".ljust(width),
        args.bam_filepath,
        "• POD5 dir:".ljust(width),
        args.pod5_dir,
        "• Output dir:".ljust(width),
        args.output_dir,
        "• Num CPUs:".ljust(width),
        args.num_processes,
        "• Log file:".ljust(width),
        log_filepath,
    )

    # Call the function with parsed arguments
    collate_bam_fast5(
        args.bam_filepath, args.pod5_dir, args.output_dir, args.num_processes
    )


if __name__ == "__main__":
    """CLI call example:
    fast5_rekindler /export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/2_bam_file/calls.bam /export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/0_pod5 /export/valenfs/data/processed_data/MinION/10_tailfindr_r10/1_package_test_data/f5r_output16 -n 100

    fast5_rekindler /export/valenfs/data/processed_data/MinION/10_tailfindr_r10/oguz_data/inputs/test.sorted.bam /export/valenfs/data/processed_data/MinION/10_tailfindr_r10/oguz_data/inputs/pod5/ /export/valenfs/data/processed_data/MinION/10_tailfindr_r10/oguz_data/output -n 100

    """
