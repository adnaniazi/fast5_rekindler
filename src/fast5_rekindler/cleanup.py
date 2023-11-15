from typing import Any

import h5py  # type: ignore


def delete_configuration_groups(group: Any) -> None:
    """
    Recursively delete groups named 'Configuration'.

    Params:
        group (h5py.Group): A group object.

    Returns:
        None
    """
    # Make a list of keys to iterate over to avoid RuntimeError for changing size during iteration
    keys = list(group.keys())
    for key in keys:
        item = group[key]
        if isinstance(item, h5py.Group):
            # If the item is a group and its name is 'Configuration', delete it
            if key == "Configuration":
                del group[key]
            else:
                # Otherwise, recursively check its subgroups
                delete_configuration_groups(item)


def cleanup_fast5(fast5_filepath: str) -> None:
    """
    Delete configuration groups from a FAST5 file.
    These groups are added automatically by ont-fast5-api
    but are not needed in rekindled FAST5 files.

    Params:
        fast5_filepath (str): Path to a FAST5 file.

    Returns:
        None
    """
    with h5py.File(fast5_filepath, "r+") as file:
        delete_configuration_groups(file)
