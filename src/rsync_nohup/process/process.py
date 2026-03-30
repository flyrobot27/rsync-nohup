#!/usr/bin/env python3
import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence
from rsync_nohup.utils.helper import log_line, build_rsync_command
from rsync_nohup.utils.exit_codes import ExitCode

_CURRENT_CHILD: subprocess.Popen | None = None
_STOP_REQUESTED = False


def _request_stop(signum, frame) -> None:
    global _STOP_REQUESTED, _CURRENT_CHILD
    _STOP_REQUESTED = True
    if _CURRENT_CHILD is not None and _CURRENT_CHILD.poll() is None:
        try:
            _CURRENT_CHILD.terminate()
        except ProcessLookupError:
            pass


def worker_main(
    source: str,
    destination: str,
    log_file: Path | None,
    max_backoff: int,
    retries: int,
    options: list[str],
) -> int:
    """
    Main worker with exponential backoff

    Args:
        source (str): path of source
        destination (str): path of destination
        log_file (Path | None): path of log file, if provided
        max_backoff (int): maximum backoff time
        retries (int): number of retries
        options (list[str]): additional rsync options

    Returns:
        int: exit code
    """
    global _CURRENT_CHILD, _STOP_REQUESTED

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGHUP, _request_stop)

    max_backoff = max(1, max_backoff)
    retries = max(0, retries)
    options = options or []

    if log_file is None:
        log_handle = open(os.devnull, "a", encoding="utf-8")
    else:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_handle = open(log_file, "a", encoding="utf-8", buffering=1)

    try:
        log_line(log_handle, f"worker pid={os.getpid()} started")
        log_line(log_handle, f"source={source}")
        log_line(log_handle, f"destination={destination}")
        log_line(log_handle, f"max_backoff={max_backoff}")
        log_line(log_handle, f"retries={retries} (0 means unlimited)")
        log_line(log_handle, f"options={options!r}")

        attempt = 1

        while True:
            if _STOP_REQUESTED:
                log_line(log_handle, "stop requested before starting next rsync attempt")
                return ExitCode.SIGTERM

            cmd = build_rsync_command(source, destination, options)
            log_line(log_handle, f"starting attempt {attempt}: {' '.join(cmd)}")

            _CURRENT_CHILD = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
            )
            returncode = _CURRENT_CHILD.wait()
            _CURRENT_CHILD = None

            if _STOP_REQUESTED:
                log_line(log_handle, f"worker stopping after child exit code {returncode}")
                return ExitCode.SIGTERM

            if returncode == 0:
                log_line(log_handle, "rsync completed successfully")
                return ExitCode.SUCCESS

            log_line(log_handle, f"rsync exited with code {returncode}")

            if returncode != ExitCode.RSYNC_RETRY:
                log_line(log_handle, "exit code is not retryable; worker exiting")
                return returncode

            if retries != 0 and attempt >= retries:
                log_line(log_handle, "retry limit reached; worker exiting")
                return returncode

            delay = min(2 ** (attempt - 1), max_backoff)
            log_line(log_handle, f"retryable failure; sleeping {delay} seconds before retry")
            time.sleep(delay)
            attempt += 1

    finally:
        log_handle.close()


def launch_worker_process(
    source: str,
    destination: str,
    log_file: Path | None,
    max_backoff: int,
    retries: int,
    options: list[str],
    run_as_root: bool = False,
) -> subprocess.Popen:
    """
    Start a detached Python worker that runs and retries rsync.
    The launcher should prompt for sudo first if run_as_root=True.

    Args:
        source (str): path to source
        destination (str): path to destination
        log_file (Path | None): path to log file
        max_backoff (int): maximum backoff time
        retries (int): number of retries
        options (list[str]): additional rsync options
        run_as_root (bool, optional): whether to run as root. Defaults to False.

    Returns:
        subprocess.Popen: the launched worker process
    """
    worker_file = Path(__file__).resolve()

    cmd = [
        sys.executable,
        str(worker_file),
        "--worker",
        source,
        destination,
        "--max-backoff",
        str(max_backoff),
        "--retries",
        str(retries),
    ]

    if log_file is not None:
        cmd.extend(["--log-file", str(log_file)])

    if options:
        cmd.append("--options")
        cmd.extend(options)

    if run_as_root and os.geteuid() != 0:
        cmd = ["sudo", "-n", *cmd]

    return subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def main(argv: Sequence[str] | None = None) -> ExitCode:
    parser = argparse.ArgumentParser(description="Internal rsync worker process")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("source", type=str)
    parser.add_argument("destination", type=str)
    parser.add_argument("--log-file", type=Path)
    parser.add_argument("--max-backoff", type=int, default=60)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--options", nargs=argparse.REMAINDER)

    args = parser.parse_args(argv)
    if not args.worker:
        print("This module is intended to be launched internally with --worker.", file=sys.stderr)
        return ExitCode.INVALID_USAGE

    options = args.options or []
    if options and options[0] == "--":
        options = options[1:]

    return worker_main(
        source=args.source,
        destination=args.destination,
        log_file=args.log_file,
        max_backoff=args.max_backoff,
        retries=args.retries,
        options=options,
    )


if __name__ == "__main__":
    raise SystemExit(main())