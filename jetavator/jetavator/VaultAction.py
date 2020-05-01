from enum import Enum


class VaultAction(Enum):
    """
    Indicates the DDL action to take given a `VaultObjectDiff`.
    """
    DROP = "drop"
    CREATE = "create"
    ALTER = "alter"
    NONE = "none"
