import os
import time
from collections import namedtuple
from functools import partial
from pathlib import Path

import click


ClickLogger = namedtuple("ClickLogger", "info success warning error")


def info(*msg, **kwargs):
    msg = " ".join(str(x) for x in msg)
    click.secho(msg, **kwargs)


log = ClickLogger(
    info,
    partial(info, fg="green"),
    partial(info, fg="yellow"),
    partial(info, fg="red"),
)


def iterdir(directory, max_depth=None, current_depth=0):
    with os.scandir(directory) as it:
        for entry in it:
            if entry.is_dir():
                if (max_depth is None) or (current_depth < max_depth):
                    yield from iterdir(
                        entry, max_depth=max_depth, current_depth=current_depth + 1
                    )
            else:
                yield Path(entry)


def fmt_size(bytes_):
    if bytes_ > 1024 * 1024:
        return f"{bytes_ / 1024 / 1024:.1f}MB"
    if bytes_ > 1024:
        return f"{bytes_ / 1024:.1f}KB"
    return f"{int(bytes_)}B"


def fmt_seconds(seconds):
    if seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    if seconds >= 60 * 60:
        minutes = seconds / 60
        return f"{int(minutes) // 60}h{int(minutes) % 60}m"
    if seconds >= 60:
        return f"{int(seconds) // 60}m{int(seconds) % 60}s"
    return f"{seconds:.1f}s"


class StopWatch:
    def __init__(self):
        self.t_start = 0.0
        self.t_stop = 0.0

    def start(self):
        self.t_start = time.time()
        return self

    def stop(self):
        self.t_stop = time.time()
        return self

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc_args):
        self.stop()
        return False

    @property
    def elapsed(self):
        if self.t_stop < self.t_start:
            raise Exception(f"StopWatch was never stopped")
        return self.t_stop - self.t_start

    def __repr__(self):
        return fmt_seconds(self.elapsed)
