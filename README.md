# rsync-nohup

`rsync-nohup` is a detached `rsync` launcher and manager for long-running transfers. It lets you start `rsync` jobs that keep running after you close the terminal, with optional logging, retry support, process listing, and stop control.

## Installation

The preferred installation method is via `pip`.

Install it from the project root:

```bash
python3 -m pip install .
```

This performs a normal installation of the current version.

If you only want to install it for your current user:

```bash
python3 -m pip install --user .
```

After installation, run it as:
```bash
rsync-nohup
```

## Usage

```bash
rsync-nohup launch SOURCE DESTINATION [--log-file LOG_FILE] [--max-backoff SECONDS] [--retries N] [--options ...]
rsync-nohup list
rsync-nohup stop PID [--force]
```

### Commands

#### `launch`

Launch a new detached `rsync` process.

```bash
rsync-nohup launch SOURCE DESTINATION [--log-file LOG_FILE] [--max-backoff SECONDS] [--retries N] [--options ...]
```

Arguments:

- `SOURCE`: source path for `rsync`
- `DESTINATION`: destination path for `rsync`
- `--log-file LOG_FILE`: optional log file path
- `--max-backoff SECONDS`: maximum retry backoff in seconds, default `60`
- `--retries N`: total number of attempts; `0` means unlimited, default `1`
- `--options ...`: extra arguments passed directly to `rsync`. Put this last.

#### `list`

List running managed `rsync` jobs and other running `rsync` processes.

```bash
rsync-nohup list
```

#### `stop`

Stop a running process by PID.

If the PID belongs to a managed child `rsync` process, `rsync-nohup` stops the parent worker instead so the transfer does not immediately restart.

```bash
rsync-nohup stop PID [--force]
```

Arguments:

- `PID`: PID of the process to stop
- `--force`: use `SIGKILL` instead of `SIGTERM`

## Examples

Launch a detached copy job with logging:

```bash
rsync-nohup launch /mnt/temp/ /mnt/temp-nas/BACKUP_OLD_PC_DATA/ --log-file ~/backup.log --options -aH --info=progress2
```

Launch with retry and exponential backoff:

```bash
rsync-nohup launch /mnt/temp/ /mnt/temp-nas/BACKUP_OLD_PC_DATA/ --log-file ~/backup.log --max-backoff 60 --retries 10 --options -aH --info=progress2
```

Launch without logging:

```bash
rsync-nohup launch /mnt/temp/ /mnt/temp-nas/BACKUP_OLD_PC_DATA/ --options -aH --info=progress2
```

List running jobs:

```bash
rsync-nohup list
```

Stop a running job gracefully:

```bash
rsync-nohup stop 12345
```

Force stop a running job:

```bash
rsync-nohup stop 12345 --force
```

Copy the contents of a directory into an existing destination directory:

```bash
rsync-nohup launch /mnt/temp/ /mnt/temp-nas/BACKUP_OLD_PC_DATA/ --log-file ~/backup.log --options -aH --info=progress2
```

Copy a directory itself into the destination:

```bash
rsync-nohup launch /mnt/temp /mnt/temp-nas/ --log-file ~/backup.log --options -aH --info=progress2
```

Exclude a subdirectory during transfer:

```bash
rsync-nohup launch /src/ /dst/ --log-file ~/rsync.log --options -aH --info=progress2 --exclude node_modules
```

## Demo

Example `list` output:

```text
rsync-nohup list
Managed rsync jobs
===================
worker=20077 rsync=20083, 20084, 20085 user=ssh-client status=running
  /mnt/temp -> /mnt/temp-nas
  log: backup.log
  cmd: rsync -a /mnt/temp /mnt/temp-nas

Other running rsync processes
=============================
None
```

Example `launch` output:

```text
rsync-nohup launch --retries 10 /mnt/temp/ /mnt/temp-nas/ --log-file ./backup.log
Launching rsync from /mnt/temp to /mnt/temp-nas with log file backup.log, max backoff 60, retries 10, and options None
Launched rsync worker with PID 19497
```

Example `stop` output:

```text
rsync-nohup stop 19658
Stopped managed worker 19658.
```
