from __future__ import annotations

from abc import ABC

from jetavator.schema_registry import Source

from .. import Job, Runner


class CreateSource(Job, ABC, register_as='create_source'):
    """
    Creates a temporary table for a source CSV file if the file exists,
    or a placeholder empty table if it does not.

    :param runner: The `Runner` that created this object.
    :param source: The `Source` object containing the source definitions.
    """

    def __init__(self, runner: Runner, source: Source) -> None:
        super().__init__(runner, source)
        self.source = source

    @property
    def name(self) -> str:
        return f'source_{self.source.name}'
