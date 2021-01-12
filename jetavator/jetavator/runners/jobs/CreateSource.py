from __future__ import annotations

from abc import ABC
from lazy_property import LazyProperty

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
        if not self.csv_exists:
            self.logger.warning(f'CSV file not found for source: {source}')

    @property
    def name(self) -> str:
        return f'create_source_{self.source.name}'

    @LazyProperty
    def csv_exists(self) -> bool:
        """
        :return: Returns True if the supplied `Source` CSV file exists on disk.
        """
        return self.runner.compute_service.source_csv_exists(self.source.name)

    @property
    def csv_path(self) -> str:
        """
        :return: Returns the path to the supplied `Source` CSV file.
        """
        return self.runner.compute_service.csv_file_path(self.source.name)
