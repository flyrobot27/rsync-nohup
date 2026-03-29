import os
import pwd
import shlex
import signal
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass


from rsync_nohup.utils.exit_codes import ExitCode


MANAGED_MODULE = "rsync_nohup.process.process"
MANAGED_SCRIPT_NAME = "process.py"


@dataclass(slots=True)
class ProcInfo:
    pid: int
    ppid: int
    uid: int
    comm: str
    cmdline: list[str]


@dataclass(slots=True)
class ManagedJob:
    worker: ProcInfo
    source: str
    destination: str
    log_file: str
    rsync_children: list[ProcInfo]


def _read_proc_status(pid: int) -> dict[str, str] | None:
    try:
        with open(f"/proc/{pid}/status", "r", encoding="utf-8", errors="replace") as f:
            data: dict[str, str] = {}
            for line in f:
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                data[key.strip()] = value.strip()
            return data
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None


def _read_proc_cmdline(pid: int) -> list[str]:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read()
        if not raw:
            return []
        return [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return []


def _all_processes() -> dict[int, ProcInfo]:
    processes: dict[int, ProcInfo] = {}

    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue

        pid = int(entry)
        status = _read_proc_status(pid)
        if status is None:
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

        processes[pid] = ProcInfo(
            pid=pid,
            ppid=ppid,
            uid=uid,
            comm=status.get("Name", ""),
            cmdline=_read_proc_cmdline(pid),
        )

    return processes


def _is_rsync_process(proc: ProcInfo) -> bool:
    if proc.comm == "rsync":
        return True
    if proc.cmdline:
        return os.path.basename(proc.cmdline[0]) == "rsync"
    return False


def _is_managed_worker(proc: ProcInfo) -> bool:
    if "--worker" not in proc.cmdline:
        return False

    argv = proc.cmdline

    if "-m" in argv:
        idx = argv.index("-m")
        if idx + 1 < len(argv) and argv[idx + 1] == MANAGED_MODULE:
            return True

    # Fallback if launched by file path instead of -m
    for token in argv[1:3]:
        name = os.path.basename(token)
        if name == MANAGED_SCRIPT_NAME:
            return True

    return False


def _children_map(processes: dict[int, ProcInfo]) -> dict[int, list[ProcInfo]]:
    children: dict[int, list[ProcInfo]] = defaultdict(list)
    for proc in processes.values():
        children[proc.ppid].append(proc)
    return children


def _descendants_of(pid: int, children_map: dict[int, list[ProcInfo]]) -> list[ProcInfo]:
    out: list[ProcInfo] = []
    seen: set[int] = set()
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


def _extract_worker_metadata(cmdline: list[str]) -> tuple[str, str, str]:
    source = "?"
    destination = "?"
    log_file = "-"

    try:
        worker_idx = cmdline.index("--worker")
        if worker_idx + 1 < len(cmdline):
            source = cmdline[worker_idx + 1]
        if worker_idx + 2 < len(cmdline):
            destination = cmdline[worker_idx + 2]
    except ValueError:
        pass

    try:
        log_idx = cmdline.index("--log-file")
        if log_idx + 1 < len(cmdline):
            log_file = cmdline[log_idx + 1]
    except ValueError:
        pass

    return source, destination, log_file


def _collect_jobs(processes: dict[int, ProcInfo]) -> tuple[list[ManagedJob], list[ProcInfo]]:
    children_map = _children_map(processes)

    workers = sorted(
        (proc for proc in processes.values() if _is_managed_worker(proc)),
        key=lambda p: p.pid,
    )

    managed_jobs: list[ManagedJob] = []
    managed_rsync_pids: set[int] = set()

    for worker in workers:
        source, destination, log_file = _extract_worker_metadata(worker.cmdline)
        descendants = _descendants_of(worker.pid, children_map)
        rsync_children = sorted(
            (proc for proc in descendants if _is_rsync_process(proc)),
            key=lambda p: p.pid,
        )

        for child in rsync_children:
            managed_rsync_pids.add(child.pid)

        managed_jobs.append(
            ManagedJob(
                worker=worker,
                source=source,
                destination=destination,
                log_file=log_file,
                rsync_children=rsync_children,
            )
        )

    unmanaged_rsync = sorted(
        (
            proc
            for proc in processes.values()
            if _is_rsync_process(proc) and proc.pid not in managed_rsync_pids
        ),
        key=lambda p: p.pid,
    )

    return managed_jobs, unmanaged_rsync


def _username(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def _short_cmdline(cmdline: list[str], width: int = 140) -> str:
    if not cmdline:
        return "(no cmdline)"
    text = shlex.join(cmdline)
    if len(text) <= width:
        return text
    return text[: width - 3] + "..."


def _render(managed_jobs: list[ManagedJob], unmanaged_rsync: list[ProcInfo]) -> str:
    lines: list[str] = []

    lines.append("Managed rsync jobs")
    lines.append("===================")
    if not managed_jobs:
        lines.append("None")
    else:
        for job in managed_jobs:
            rsync_pids = ", ".join(str(p.pid) for p in job.rsync_children) or "-"
            status = "running" if job.rsync_children else "waiting/backoff"
            lines.append(
                f"worker={job.worker.pid} rsync={rsync_pids} "
                f"user={_username(job.worker.uid)} status={status}"
            )
            lines.append(f"  {job.source} -> {job.destination}")
            lines.append(f"  log: {job.log_file}")
            if job.rsync_children:
                lines.append(f"  cmd: {_short_cmdline(job.rsync_children[0].cmdline)}")
            else:
                lines.append(f"  cmd: {_short_cmdline(job.worker.cmdline)}")
            lines.append("")

    lines.append("Other running rsync processes")
    lines.append("=============================")
    if not unmanaged_rsync:
        lines.append("None")
    else:
        for proc in unmanaged_rsync:
            lines.append(
                f"pid={proc.pid} ppid={proc.ppid} user={_username(proc.uid)}"
            )
            lines.append(f"  cmd: {_short_cmdline(proc.cmdline)}")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _clear_screen() -> None:
    print("\033[2J\033[H", end="")


def _pid_exists(pid: int) -> bool:
    return os.path.exists(f"/proc/{pid}")


def _send_signal(proc: ProcInfo, sig: signal.Signals) -> None:
    current_uid = os.geteuid()

    if current_uid == 0 or current_uid == proc.uid:
        os.kill(proc.pid, sig)
        return

    sig_name = sig.name[3:] if sig.name.startswith("SIG") else sig.name
    subprocess.run(
        ["sudo", "kill", "-s", sig_name, str(proc.pid)],
        check=True,
        stdin=subprocess.DEVNULL,
    )


def _wait_for_exit(pid: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(0.25)
    return not _pid_exists(pid)


def _resolve_stop_target(
    pid: int,
    managed_jobs: list[ManagedJob],
    unmanaged_rsync: list[ProcInfo],
) -> tuple[ProcInfo, str] | None:
    for job in managed_jobs:
        if pid == job.worker.pid:
            return job.worker, f"managed worker {job.worker.pid}"

        for child in job.rsync_children:
            if pid == child.pid:
                return (
                    job.worker,
                    f"managed worker {job.worker.pid} (resolved from child rsync {child.pid})",
                )

    for proc in unmanaged_rsync:
        if pid == proc.pid:
            return proc, f"rsync {proc.pid}"

    return None


def list_processes(watch_interval: float | None) -> ExitCode:
    """
    List:
    1) managed rsync jobs started by this tool
    2) other currently running rsync processes

    watch_interval:
      - None / <= 0: print once
      - > 0: refresh until Ctrl-C
    """
    try:
        while True:
            processes = _all_processes()
            managed_jobs, unmanaged_rsync = _collect_jobs(processes)
            output = _render(managed_jobs, unmanaged_rsync)

            if watch_interval is not None and watch_interval > 0:
                _clear_screen()
                print(time.strftime("%Y-%m-%d %H:%M:%S"))
                print()
                print(output, end="")
                time.sleep(watch_interval)
            else:
                print(output, end="")
                return ExitCode.SUCCESS

    except KeyboardInterrupt:
        return ExitCode.SUCCESS
    except Exception as exc:
        print(f"Error listing processes: {exc}")
        return ExitCode.ERROR


def stop_process(pid: int, force: bool) -> ExitCode:
    """
    Stop a process by PID.

    If PID belongs to a managed rsync child, this stops the parent worker instead,
    so the worker does not immediately retry the rsync.
    """
    try:
        processes = _all_processes()
        managed_jobs, unmanaged_rsync = _collect_jobs(processes)

        resolved = _resolve_stop_target(pid, managed_jobs, unmanaged_rsync)
        if resolved is None:
            print(f"Error: PID {pid} is not a running managed worker or rsync process.")
            return ExitCode.ERROR

        target_proc, description = resolved
        sig = signal.SIGKILL if force else signal.SIGTERM

        _send_signal(target_proc, sig)

        if force:
            print(f"Sent SIGKILL to {description}.")
            return ExitCode.SUCCESS

        if _wait_for_exit(target_proc.pid, timeout=10.0):
            print(f"Stopped {description}.")
            return ExitCode.SUCCESS

        print(f"Sent SIGTERM to {description}, but it is still running after 10 seconds.")
        print("Try again with --force if needed.")
        return ExitCode.ERROR

    except subprocess.CalledProcessError as exc:
        print(f"Error stopping process: {exc}")
        return ExitCode.ERROR
    except PermissionError as exc:
        print(f"Permission error stopping process: {exc}")
        return ExitCode.ERROR
    except ProcessLookupError:
        print("Process already exited.")
        return ExitCode.SUCCESS
    except Exception as exc:
        print(f"Error stopping process: {exc}")
        return ExitCode.ERROR
