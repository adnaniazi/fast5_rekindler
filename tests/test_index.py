import os
import tempfile

from fast5_rekindler.index import generate_fast5_file_paths, prepare_data


def test_generate_fast5_file_paths() -> None:
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Add dummy files
        fast5_files = ["file1.fast5", "file2.fast5"]
        other_files = ["file1.txt", "file2.csv"]
        for file in fast5_files + other_files:
            open(os.path.join(temp_dir, file), "a").close()

        # Test the function
        generated_paths = list(generate_fast5_file_paths(temp_dir))

        # Assert that only fast5 file paths are returned
        assert len(generated_paths) == len(fast5_files)
        for path in generated_paths:
            assert os.path.basename(path) in fast5_files
            assert path.startswith(temp_dir)
            assert path.endswith(".fast5")


def test_prepare_data() -> None:
    # Sample input
    read_ids = ["read1", "read2", "read3"]
    fast5_filepath = "/path/to/fast5_file.fast5"

    # Expected output
    expected_output = [
        ("read1", fast5_filepath),
        ("read2", fast5_filepath),
        ("read3", fast5_filepath),
    ]

    # Call the function
    result = prepare_data(read_ids, fast5_filepath)

    # Assert the result is as expected
    assert result == expected_output
