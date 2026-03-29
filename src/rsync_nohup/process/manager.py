
from rsync_nohup.utils.exit_codes import ExitCode


def list_processes(watch_interval: float | None) -> ExitCode:
    return ExitCode.SUCCESS

def stop_process(pid: int, force: bool) -> ExitCode:
    return ExitCode.SUCCESS