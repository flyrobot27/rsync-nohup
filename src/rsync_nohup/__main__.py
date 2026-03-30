import argparse
from collections.abc import Sequence
from pathlib import Path
from rsync_nohup.launcher.launcher import launch_rsync
from rsync_nohup.process.manager import list_processes, stop_process
from rsync_nohup.utils.exit_codes import ExitCode

def main(argv: Sequence[str] | None = None) -> ExitCode:
    parser = argparse.ArgumentParser(description="Detached rsync launcher and manager with logging, retry, process listing, and stop control.")

    # subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", required=True)
    # launch command
    launch_parser = subparsers.add_parser("launch", help="Launch a new rsync process")
    launch_parser.add_argument("source",  type=Path, help="Source path for rsync")
    launch_parser.add_argument("destination", type=Path, help="Destination path for rsync")
    launch_parser.add_argument("--log-file", type=Path, help="Path to log file for rsync output. If not specified, no logging is performed.", default=None)
    launch_parser.add_argument("--max-backoff", type=int, help="Maximum backoff time in seconds for retries (default: 60)", default=60)
    launch_parser.add_argument("--retries", type=int, help="Number of retry attempts for failed rsync processes. 0 means unlimited. Default to 1 (no retries)", default=1)
    launch_parser.add_argument("--options", nargs=argparse.REMAINDER, help="Additional arguments to pass to rsync (e.g., -avz, --exclude, etc.). Using `-a` recommended. Ensure to put this last.")

    # list command
    subparsers.add_parser("list", help="List all running rsync processes")

    # stop command
    stop_parser = subparsers.add_parser("stop", help="Stop a running rsync process")
    stop_parser.add_argument("pid", type=int, help="PID of the rsync process to stop")
    stop_parser.add_argument("--force", action="store_true", help="Force stop the process (use SIGKILL instead of SIGTERM)")

    args = parser.parse_args(argv)

    match args.command:
        case "launch":
            return launch_rsync(args.source, args.destination, args.log_file, args.max_backoff, args.retries, args.options)
        case "list":
            return list_processes()
        case "stop":
            return stop_process(args.pid, args.force)
        case _:
            parser.error("Unknown command")
            return ExitCode.INVALID_ARGUMENTS

if __name__ == "__main__":
    raise SystemExit(main())