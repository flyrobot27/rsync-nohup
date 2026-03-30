import time
from io import TextIOWrapper
from pathlib import Path


def log_line(handle: TextIOWrapper, message: str) -> None:
    """
    log a line with timestamp to the given handle

    Args:
        handle (TextIOWrapper): The file handle to write the log message to.
        message (str): The message to log.
    """
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    handle.write(f"[{timestamp}] {message}\n")
    handle.flush()

def build_rsync_command(source: str, destination: str, options: list[str]) -> list[str]:
    """
    Build the rsync command given the options, and resolve source and destination paths to absolute paths.

    Args:
        source (str): The source path for rsync.
        destination (str): The destination path for rsync.
        options (list[str]): A list of additional rsync options.

    Returns:
        list[str]: The constructed rsync command.
    """
    return ["rsync", *options, source, destination]
