import os
from pathlib import Path
from utils.access import is_sudo_required, rsync_locations_accessible

def launch_rsync(source: Path, destination: Path, log_file: Path | None, max_backoff: int, retries: int, options: list[str]) -> int:
    """
    launch rsync worker process

    Args:
        source (Path): path to source
        destination (Path): path to destination
        log_file (Path | None): path to log file
        max_backoff (int): maximum backoff time
        retries (int): number of retries
        options (list[str]): additional rsync options

    Returns:
        int: exit code
    """
    # check if source and destination are accessible
    accessible, error_message = rsync_locations_accessible(source, destination)
    if not accessible:
        print(f"Error: {error_message}")
        return 1

    if log_file is not None:
        # try to create the log file if it doesn't exist, and check if it's writable
        try:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            if not log_file.exists():
                log_file.touch()
            if not log_file.is_file() or not os.access(log_file, os.W_OK):
                print(f"Error: Log file {log_file} is not writable.")
                return 1
        except Exception as e:
            print(f"Error: Could not create or access log file {log_file}: {e}")
            return 1
    
    sudo_required = is_sudo_required(source, destination)

    # Build the rsync command
    rsync_command = ["rsync", str(source), str(destination)] + options

