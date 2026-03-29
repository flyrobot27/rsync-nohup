import os
from pathlib import Path
from utils.access import is_sudo_required, rsync_locations_accessible
from process.process import launch_worker_process
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
    
    try:
        proc = launch_worker_process(
            source=source,
            destination=destination,
            log_file=log_file,
            max_backoff=max_backoff,
            retries=retries,
            options=options,
            run_as_root=sudo_required,
        )
        print(f"Launched rsync worker with PID {proc.pid}")
        return 0
    except Exception as e:
        print(f"Error launching rsync worker: {e}")
        return 1
