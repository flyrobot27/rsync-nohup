#!/usr/bin/env python3
import argparse
import os
import pwd
import re
import shlex
import signal
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

DEFAULT_RSYNC_ARGS = ["-aH", "--info=progress2"]
RETRYABLE_EXIT_CODES = {20}  # rsync "received SIGINT, SIGTERM, or SIGHUP"
SCRIPT_PATH = os.path.realpath(__file__)

CURRENT_CHILD = None
STOP_REQUESTED = False


@dataclass
class ProcInfo:
    pid: int
    ppid: int
    uid: int
    comm: str
    cmdline: List[str]


@dataclass
class ManagedJob:
    worker: ProcInfo
    source: str
    target: str
    log_path: str
    rsync_children: List[ProcInfo]


def sanitize_for_log(path: str) -> str:
    path = path.rstrip("/\\") or "root"
    path = path.replace("/", "_").replace("\\", "_").replace(" ", "_").replace(":", "_")
    path = re.sub(r"[^A-Za-z0-9._-]+", "_", path)
    path = path.strip("._-") or "path"
    return path


def default_log_path(source: str, target: str) -> str:
    return os.path.abspath(
        f"./rsync-{sanitize_for_log(source)}-{sanitize_for_log(target)}.log"
    )


def first_existing_parent(path: str) -> str:
    current = os.path.abspath(path)
    while not os.path.exists(current):
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return current


def basic_needs_sudo(source: str, target: str) -> bool:
    if os.geteuid() == 0:
        return False

    if not os.path.exists(source):
        raise FileNotFoundError(f"Source does not exist: {source}")

    if not os.access(source, os.R_OK):
        return True

    if os.path.isdir(source) and not os.access(source, os.X_OK):
        return True

    if os.path.exists(target):
        target_check = target
    else:
        target_check = first_existing_parent(target)

    if not os.access(target_check, os.W_OK):
        return True

    if os.path.isdir(target_check) and not os.access(target_check, os.X_OK):
        return True

    return False


def permission_error(output: str) -> bool:
    text = output.lower()
    indicators = [
        "permission denied",
        "operation not permitted",
        "access denied",
    ]
    return any(token in text for token in indicators)


def build_rsync_cmd(source: str, target: str, extra_args: List[str], dry_run: bool = False) -> List[str]:
    cmd = ["rsync", *DEFAULT_RSYNC_ARGS]
    if dry_run:
        cmd.append("-n")
    cmd.extend(extra_args)
    cmd.extend(["--", source, target])
    return cmd


def run_preflight(source: str, target: str, extra_args: List[str], use_sudo: bool = False) -> Tuple[int, str]:
    cmd = build_rsync_cmd(source, target, extra_args, dry_run=True)
    if use_sudo and os.geteuid() != 0:
        cmd = ["sudo", "-n", *cmd]

    proc = subprocess.run(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.returncode, proc.stdout


def should_retry(returncode: int) -> bool:
    return returncode in RETRYABLE_EXIT_CODES


def log_line(handle, message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    handle.write(f"[{timestamp}] {message}\n")
    handle.flush()


def request_stop(signum, frame) -> None:
    global STOP_REQUESTED, CURRENT_CHILD
    STOP_REQUESTED = True
    if CURRENT_CHILD is not None and CURRENT_CHILD.poll() is None:
        try:
            CURRENT_CHILD.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass


def user_name(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def safe_realpath(path: str) -> str:
    try:
        return os.path.realpath(path)
    except Exception:
        return path


def read_proc_status(pid: int) -> Optional[Dict[str, str]]:
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8", errors="replace") as f:
            data = {}
            for line in f:
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                data[key.strip()] = value.strip()
            return data
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None


def read_proc_cmdline(pid: int) -> List[str]:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read()
        if not raw:
            return []
        return [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return []


def list_processes() -> Dict[int, ProcInfo]:
    processes: Dict[int, ProcInfo] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        status = read_proc_status(pid)
        if not status:
            continue

        try:
            ppid = int(status.get("PPid", "0"))
        except ValueError:
            ppid = 0

        uid_field = status.get("Uid", "0")
        try:
            uid = int(uid_field.split()[0])
        except (ValueError, IndexError):
            uid = 0

        comm = status.get("Name", "")
        cmdline = read_proc_cmdline(pid)

        processes[pid] = ProcInfo(
            pid=pid,
            ppid=ppid,
            uid=uid,
            comm=comm,
            cmdline=cmdline,
        )
    return processes


def is_rsync_process(proc: ProcInfo) -> bool:
    if proc.comm == "rsync":
        return True
    if proc.cmdline:
        return os.path.basename(proc.cmdline[0]) == "rsync"
    return False


def is_worker_process(proc: ProcInfo) -> bool:
    if len(proc.cmdline) < 3:
        return False
    script_arg = safe_realpath(proc.cmdline[1])
    subcommand = proc.cmdline[2]
    return script_arg == SCRIPT_PATH and subcommand == "worker"


def build_children_map(processes: Dict[int, ProcInfo]) -> Dict[int, List[ProcInfo]]:
    children: Dict[int, List[ProcInfo]] = defaultdict(list)
    for proc in processes.values():
        children[proc.ppid].append(proc)
    return children


def descendants_of(pid: int, children_map: Dict[int, List[ProcInfo]]) -> List[ProcInfo]:
    out: List[ProcInfo] = []
    seen = set()
    stack = [pid]
    while stack:
        parent = stack.pop()
        for child in children_map.get(parent, []):
            if child.pid in seen:
                continue
            seen.add(child.pid)
            out.append(child)
            stack.append(child.pid)
    return out


def extract_worker_metadata(cmdline: Sequence[str]) -> Tuple[str, str, str]:
    # cmdline layout:
    # python script worker [options...] source target [-- extra rsync args...]
    tokens = list(cmdline[3:])
    i = 0
    options_with_values = {
        "-l",
        "--log",
        "--initial-backoff",
        "--max-backoff",
        "--max-retries",
    }

    log_path = "?"
    source = "?"
    target = "?"

    while i < len(tokens):
        tok = tokens[i]

        if tok == "--":
            break

        if tok in options_with_values:
            if i + 1 < len(tokens) and tok in {"-l", "--log"}:
                log_path = tokens[i + 1]
            i += 2
            continue

        source = tok
        if i + 1 < len(tokens):
            target = tokens[i + 1]
        break

    return source, target, log_path


def collect_jobs(processes: Dict[int, ProcInfo]) -> Tuple[List[ManagedJob], List[ProcInfo]]:
    children_map = build_children_map(processes)
    workers = sorted(
        [proc for proc in processes.values() if is_worker_process(proc)],
        key=lambda p: p.pid,
    )

    managed_jobs: List[ManagedJob] = []
    managed_rsync_pids = set()

    for worker in workers:
        source, target, log_path = extract_worker_metadata(worker.cmdline)
        descendants = descendants_of(worker.pid, children_map)
        rsync_children = [proc for proc in descendants if is_rsync_process(proc)]
        for proc in rsync_children:
            managed_rsync_pids.add(proc.pid)

        managed_jobs.append(
            ManagedJob(
                worker=worker,
                source=source,
                target=target,
                log_path=log_path,
                rsync_children=sorted(rsync_children, key=lambda p: p.pid),
            )
        )

    unmanaged_rsync = sorted(
        [
            proc
            for proc in processes.values()
            if is_rsync_process(proc) and proc.pid not in managed_rsync_pids
        ],
        key=lambda p: p.pid,
    )

    return managed_jobs, unmanaged_rsync


def short_cmdline(cmdline: Sequence[str], width: int = 140) -> str:
    if not cmdline:
        return "(no cmdline available)"
    text = shlex.join(list(cmdline))
    if len(text) <= width:
        return text
    return text[: width - 3] + "..."


def render_jobs(managed_jobs: List[ManagedJob], unmanaged_rsync: List[ProcInfo]) -> str:
    lines: List[str] = []

    lines.append("Managed rsync jobs")
    lines.append("===================")
    if not managed_jobs:
        lines.append("None")
    else:
        for idx, job in enumerate(managed_jobs, 1):
            rsync_pids = ", ".join(str(p.pid) for p in job.rsync_children) or "-"
            status = "running" if job.rsync_children else "waiting/backoff"
            lines.append(
                f"[M{idx}] worker={job.worker.pid} rsync={rsync_pids} user={user_name(job.worker.uid)} status={status}"
            )
            lines.append(f"     {job.source} -> {job.target}")
            lines.append(f"     log: {job.log_path}")
            if job.rsync_children:
                lines.append(f"     cmd: {short_cmdline(job.rsync_children[0].cmdline)}")
            else:
                lines.append(f"     cmd: {short_cmdline(job.worker.cmdline)}")
            lines.append("")

    lines.append("Other running rsync processes")
    lines.append("=============================")
    if not unmanaged_rsync:
        lines.append("None")
    else:
        for idx, proc in enumerate(unmanaged_rsync, 1):
            lines.append(
                f"[R{idx}] pid={proc.pid} ppid={proc.ppid} user={user_name(proc.uid)}"
            )
            lines.append(f"     cmd: {short_cmdline(proc.cmdline)}")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def clear_screen() -> None:
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def pid_exists(pid: int) -> bool:
    return os.path.exists(f"/proc/{pid}")


def ensure_sudo() -> None:
    subprocess.run(["sudo", "-v"], check=True)


def send_signal_to_pid(pid: int, sig: signal.Signals, uid: int) -> None:
    current_uid = os.geteuid()
    if current_uid == 0 or current_uid == uid:
        os.kill(pid, sig)
        return

    ensure_sudo()
    subprocess.run(
        ["sudo", "kill", f"-{sig.name}", str(pid)],
        check=True,
        stdin=subprocess.DEVNULL,
    )


def wait_for_exit(pid: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not pid_exists(pid):
            return True
        time.sleep(0.25)
    return not pid_exists(pid)


def resolve_stop_target(
    pid: int,
    managed_jobs: List[ManagedJob],
    unmanaged_rsync: List[ProcInfo],
) -> Optional[Tuple[str, int, int, str]]:
    for job in managed_jobs:
        if pid == job.worker.pid:
            return ("managed", job.worker.pid, job.worker.uid, f"worker {job.worker.pid} ({job.source} -> {job.target})")
        for child in job.rsync_children:
            if pid == child.pid:
                return (
                    "managed",
                    job.worker.pid,
                    job.worker.uid,
                    f"worker {job.worker.pid} for rsync child {child.pid} ({job.source} -> {job.target})",
                )

    for proc in unmanaged_rsync:
        if pid == proc.pid:
            return ("unmanaged", proc.pid, proc.uid, f"rsync {proc.pid}")

    return None


def stop_target(
    kind: str,
    pid: int,
    uid: int,
    description: str,
    force: bool = False,
) -> int:
    sig = signal.SIGKILL if force else signal.SIGTERM
    send_signal_to_pid(pid, sig, uid)

    if force:
        print(f"Sent SIGKILL to {description}.")
        return 0

    if wait_for_exit(pid, timeout=10.0):
        print(f"Stopped {description}.")
    else:
        print(f"Sent SIGTERM to {description}, but it is still running after 10 seconds.")
        print("It may still be shutting down. Recheck with: list")
        print("If needed, run stop again with --force.")
    return 0


def handle_list(args: argparse.Namespace) -> int:
    try:
        while True:
            processes = list_processes()
            managed_jobs, unmanaged_rsync = collect_jobs(processes)
            output = render_jobs(managed_jobs, unmanaged_rsync)

            if args.watch and args.watch > 0:
                clear_screen()
                print(time.strftime("%Y-%m-%d %H:%M:%S"))
                print()
                print(output, end="")
                time.sleep(args.watch)
            else:
                print(output, end="")
                return 0
    except KeyboardInterrupt:
        return 130


def handle_stop(args: argparse.Namespace) -> int:
    processes = list_processes()
    managed_jobs, unmanaged_rsync = collect_jobs(processes)

    if args.pid is not None:
        resolved = resolve_stop_target(args.pid, managed_jobs, unmanaged_rsync)
        if not resolved:
            print(f"PID {args.pid} is not a running managed worker or rsync process.")
            return 1

        kind, stop_pid, stop_uid, description = resolved
        if kind == "managed":
            print("That PID belongs to a managed job, so the worker will be stopped to prevent automatic retry.")
        return stop_target(kind, stop_pid, stop_uid, description, force=args.force)

    choices: List[Tuple[str, int, int, str]] = []

    for job in managed_jobs:
        desc = f"worker {job.worker.pid} ({job.source} -> {job.target})"
        choices.append(("managed", job.worker.pid, job.worker.uid, desc))

    for proc in unmanaged_rsync:
        desc = f"rsync {proc.pid}"
        choices.append(("unmanaged", proc.pid, proc.uid, desc))

    if not choices:
        print("No running managed jobs or rsync processes found.")
        return 0

    print(render_jobs(managed_jobs, unmanaged_rsync), end="")
    print("Stop menu")
    print("=========")
    for i, (kind, pid, uid, desc) in enumerate(choices, 1):
        label = "managed worker" if kind == "managed" else "rsync"
        print(f"[{i}] {label}: {desc}")

    print()
    selection = input("Choose a number to stop, or 'q' to cancel: ").strip()
    if selection.lower() == "q":
        print("Cancelled.")
        return 0

    try:
        index = int(selection)
    except ValueError:
        print("Invalid selection.")
        return 1

    if index < 1 or index > len(choices):
        print("Selection out of range.")
        return 1

    kind, pid, uid, desc = choices[index - 1]

    if kind == "managed":
        print("Stopping the worker, not just the child rsync, so it does not restart automatically.")

    if not args.yes:
        confirm = input(f"Stop {desc}? [y/N]: ").strip().lower()
        if confirm not in {"y", "yes"}:
            print("Cancelled.")
            return 0

    return stop_target(kind, pid, uid, desc, force=args.force)


def worker_main(args: argparse.Namespace) -> int:
    global CURRENT_CHILD, STOP_REQUESTED

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGHUP, request_stop)

    extra_args = list(args.rsync_args or [])
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    os.makedirs(os.path.dirname(args.log), exist_ok=True)

    with open(args.log, "a", encoding="utf-8", buffering=1) as log_handle:
        log_line(log_handle, f"Worker started (pid={os.getpid()}, uid={os.geteuid()}).")
        log_line(log_handle, f"Source: {args.source}")
        log_line(log_handle, f"Target: {args.target}")
        log_line(
            log_handle,
            f"Backoff: initial={args.initial_backoff}s, max={args.max_backoff}s, max_retries={args.max_retries or 'unlimited'}",
        )

        attempt = 1
        while True:
            if STOP_REQUESTED:
                log_line(log_handle, "Stop requested before starting next rsync attempt. Exiting worker.")
                return 143

            cmd = build_rsync_cmd(args.source, args.target, extra_args, dry_run=False)
            log_line(log_handle, f"Starting attempt {attempt}: {shlex.join(cmd)}")

            CURRENT_CHILD = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                close_fds=True,
            )
            returncode = CURRENT_CHILD.wait()
            CURRENT_CHILD = None

            if STOP_REQUESTED:
                log_line(log_handle, f"Worker stop requested. Child exited with code {returncode}. No retry will be attempted.")
                return 143

            if returncode == 0:
                log_line(log_handle, "rsync completed successfully.")
                return 0

            log_line(log_handle, f"rsync exited with code {returncode}.")

            if not should_retry(returncode):
                log_line(log_handle, "Exit code is not retryable. Stopping.")
                return returncode

            if args.max_retries > 0 and attempt >= args.max_retries:
                log_line(log_handle, f"Retry limit reached ({args.max_retries}). Stopping.")
                return returncode

            delay = min(args.initial_backoff * (2 ** (attempt - 1)), args.max_backoff)
            log_line(log_handle, f"Retryable failure detected. Sleeping {delay} seconds before retry.")
            time.sleep(delay)
            attempt += 1


def add_common_transfer_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-l", "--log", help="Path to the rsync log file.")
    parser.add_argument(
        "--initial-backoff",
        type=int,
        default=30,
        help="Initial retry delay in seconds. Default: 30",
    )
    parser.add_argument(
        "--max-backoff",
        type=int,
        default=1800,
        help="Maximum retry delay in seconds. Default: 1800",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=0,
        help="Maximum number of attempts for retryable failures. 0 means unlimited. Default: 0",
    )
    parser.add_argument("source")
    parser.add_argument("target")
    parser.add_argument(
        "rsync_args",
        nargs=argparse.REMAINDER,
        help="Extra arguments passed through to rsync. Put them after SOURCE TARGET, optionally after --.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Detached rsync launcher with logging, retry, process listing, and stop control."
    )
    subparsers = parser.add_subparsers(dest="command")

    start_parser = subparsers.add_parser("start", help="Start a detached rsync worker")
    add_common_transfer_args(start_parser)

    list_parser = subparsers.add_parser("list", help="List running managed jobs and rsync processes")
    list_parser.add_argument(
        "--watch",
        type=float,
        default=0,
        help="Refresh every N seconds until interrupted.",
    )

    stop_parser = subparsers.add_parser("stop", help="Interactively stop a managed job or rsync process")
    stop_parser.add_argument(
        "--pid",
        type=int,
        help="Stop a specific PID. If it is a managed rsync child, the parent worker will be stopped instead.",
    )
    stop_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip confirmation prompt.",
    )
    stop_parser.add_argument(
        "--force",
        action="store_true",
        help="Use SIGKILL instead of SIGTERM.",
    )

    worker_parser = subparsers.add_parser("worker", help=argparse.SUPPRESS)
    add_common_transfer_args(worker_parser)

    return parser


def normalize_argv(argv: Sequence[str]) -> List[str]:
    subcommands = {"start", "list", "stop", "worker", "-h", "--help"}
    if len(argv) > 1 and argv[1] not in subcommands:
        return [argv[0], "start", *argv[1:]]
    return list(argv)


def launch_worker(args: argparse.Namespace) -> int:
    extra_args = list(args.rsync_args or [])
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    log_path = os.path.abspath(args.log) if args.log else default_log_path(args.source, args.target)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    need_sudo = basic_needs_sudo(args.source, args.target)

    if not need_sudo and os.geteuid() != 0:
        rc, output = run_preflight(args.source, args.target, extra_args, use_sudo=False)
        if rc == 0:
            need_sudo = False
        elif permission_error(output):
            need_sudo = True
        else:
            sys.stderr.write("Preflight rsync failed before launch:\n")
            sys.stderr.write(output)
            return rc or 1

    if need_sudo and os.geteuid() != 0:
        print("Permission check suggests sudo is required. Prompting now...")
        ensure_sudo()

        rc, output = run_preflight(args.source, args.target, extra_args, use_sudo=True)
        if rc != 0:
            sys.stderr.write("Preflight rsync with sudo failed before launch:\n")
            sys.stderr.write(output)
            return rc or 1

    script_path = SCRIPT_PATH
    worker_cmd = [
        sys.executable,
        script_path,
        "worker",
        "--log",
        log_path,
        "--initial-backoff",
        str(args.initial_backoff),
        "--max-backoff",
        str(args.max_backoff),
        "--max-retries",
        str(args.max_retries),
        args.source,
        args.target,
    ]

    if extra_args:
        worker_cmd.append("--")
        worker_cmd.extend(extra_args)

    if need_sudo and os.geteuid() != 0:
        worker_cmd = ["sudo", "-n", *worker_cmd]

    with open(os.devnull, "rb") as devnull_in, open(os.devnull, "ab") as devnull_out:
        proc = subprocess.Popen(
            worker_cmd,
            stdin=devnull_in,
            stdout=devnull_out,
            stderr=devnull_out,
            start_new_session=True,
            close_fds=True,
        )

    print("Started detached worker.")
    print(f"Worker PID: {proc.pid}")
    print(f"Log: {log_path}")
    print()
    print("Useful commands:")
    print(f'  {shlex.quote(sys.argv[0])} list')
    print(f'  tail -f "{log_path}"')
    print(f'  {shlex.quote(sys.argv[0])} stop')
    print(f'  {shlex.quote(sys.argv[0])} stop --pid {proc.pid}')

    if need_sudo and os.geteuid() != 0:
        print()
        print("Note: because sudo was required, the detached worker runs as root.")

    return 0


def main() -> int:
    argv = normalize_argv(sys.argv)
    parser = build_parser()
    args = parser.parse_args(argv[1:])

    try:
        if args.command == "start":
            return launch_worker(args)
        if args.command == "worker":
            return worker_main(args)
        if args.command == "list":
            return handle_list(args)
        if args.command == "stop":
            return handle_stop(args)

        parser.print_help()
        return 1

    except FileNotFoundError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(f"Command failed: {exc}\n")
        return exc.returncode or 1
    except KeyboardInterrupt:
        sys.stderr.write("Interrupted.\n")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())