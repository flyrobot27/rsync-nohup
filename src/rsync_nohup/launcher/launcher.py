from pathlib import Path
def launch_rsync(source: Path, destination: Path, log_file: Path, max_backoff: int, retries: int, options: list[str]) -> int:
    return 0