import os
from pathlib import Path
from typing import Tuple

def rsync_locations_accessible(source: Path, destination: Path) -> Tuple[bool, str | None]:
    """
    Check if source and destination is accessible and exists

    Args:
        source (Path): path to source
        destination (Path): path to destination

    Returns:
        Tuple[bool, str | None]: True if exists, False otherwise. If False, returns error message.
    """
    if not source.exists():
        return False, f"Source path {source} does not exist."

    if not source.is_dir() and not source.is_file():
        return False, f"Source path {source} is not a file or directory."

    if not destination.exists():
        return False, f"Destination path {destination} does not exist."

    if not destination.is_dir() and not destination.is_file():
        return False, f"Destination path {destination} is not a file or directory."

    return True, None


def is_sudo_required(source: Path, destination: Path) -> bool:
    if os.geteuid() == 0:
        return False  # Already running as root, no need for sudo

    # check if source is readable
    if not os.access(source, os.R_OK):
        return True
    
    # check if destination is writable
    if destination.exists():
        if not os.access(destination, os.W_OK):
            return True
    else:
        # check if we can write to the parent directory of the destination
        parent_dir = destination.parent
        if not os.access(parent_dir, os.W_OK):
            return True

    return False