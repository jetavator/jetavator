from enum import Enum


class LoadType(Enum):
    DELTA = 'delta'
    FULL = 'full'