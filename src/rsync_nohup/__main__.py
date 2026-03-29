import argparse

def main() -> int:
    parser = argparse.ArgumentParser(description="Detached rsync launcher and manager with logging, retry, process listing, and stop control.")

    # subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", required=True)
    # launch command
    launch_parser = subparsers.add_parser("launch", help="Launch a new rsync process")
    launch_parser.add_argument("source", help="Source path for rsync")
    launch_parser.add_argument("destination", help="Destination path for rsync")
    launch_parser.add_argument("--log-file", help="Path to log file for rsync output", default="rsync_nohup.log")
    launch_parser.add_argument("--max-backoff", type=int, help="Maximum backoff time in seconds for retries (default: 60)", default=60)
    launch_parser.add_argument("--retries", type=int, help="Number of retry attempts for failed rsync processes. 0 means unlimited. Default to 1 (no retries)", default=1)
    launch_parser.add_argument("--options", help="Additional rsync options", default="")

    # list command
    list_parser = subparsers.add_parser("list", help="List all running rsync processes")
    list_parser.add_argument("--watch", type=float, help="Watch mode: refresh the list every N seconds. Default to 1 second if --watch is provided without a value.", nargs="?", const=1.0)

    # stop command
    stop_parser = subparsers.add_parser("stop", help="Stop a running rsync process")
    stop_parser.add_argument("pid", type=int, help="PID of the rsync process to stop")
    stop_parser.add_argument("--force", action="store_true", help="Force stop the process (use SIGKILL instead of SIGTERM)")

    args = parser.parse_args()

if __name__ == "__main__":
    try:
        status = main()
    except KeyboardInterrupt:
        exit()
    else:
        raise SystemExit(status)