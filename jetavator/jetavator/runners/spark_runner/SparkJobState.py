from enum import Enum, auto


class SparkJobState(Enum):
    BLOCKED = auto()
    READY = auto()
    RUNNING = auto()
    FINISHED = auto()
    ACKNOWLEDGED = auto()
