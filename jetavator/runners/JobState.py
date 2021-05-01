from enum import IntEnum


class JobState(IntEnum):
    """
    Indicates the current execution state of a `SparkJob`.
    """
    BLOCKED = 1
    READY = 2
    RUNNING = 3
    FINISHED = 4
    ACKNOWLEDGED = 5
