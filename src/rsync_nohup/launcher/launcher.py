import os
from pathlib import Path
from utils.access import is_sudo_required, rsync_locations_accessible
import subprocess

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

    sudo_required = is_sudo_required(source, destination)

    # Build the rsync command
    cmd = ["rsync", *options, str(source), str(destination)]

    if sudo_required:
        try:
            print("Sudo is required")
            subprocess.run(["sudo", "-v"], check=True)
        except subprocess.CalledProcessError:
            print("Error: Sudo access is required but not granted.")
            return 1
        
        cmd = ["sudo", "-n", *cmd]
    
    # create log file if specified and check if it's writable
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
    
    # launch the rsync command in a detached process
    try:
        if log_file is None:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )
        else:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            with open(log_file, "ab", buffering=0) as log_handle:
                proc = subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                )

        print(f"Started detached rsync process. PID: {proc.pid}")
        return 0

    except Exception as e:
        print(f"Error: Failed to start rsync: {e}")
        return 1
