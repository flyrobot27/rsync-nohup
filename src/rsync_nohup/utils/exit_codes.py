from enum import IntEnum

class ExitCode(IntEnum):
    """Exit codes for rsync-nohup application."""
    SUCCESS = 0
    GENERIC_ERROR = 1
    INVALID_USAGE = 2
    RSYNC_RETRY = 20
    INTERRUPT = 120
    SIGTERM = 143