from abc import ABC
from typing import List

from jetavator.schema_registry import Source

from .. import Job, Runner


class DropSource(Job, ABC, register_as='drop_source'):
    """
    Drop the temporary table for a source CSV file.

    :param runner: The `Runner` that created this object.
    :param source: The `Source` object containing the source definitions.
    """

    def __init__(self, runner: Runner, source: Source) -> None:
        super().__init__(runner, source)
        self.source = source

    @property
    def name(self) -> str:
        return f'drop_source_{self.source.name}'

    @property
    def dependencies(self) -> List[Job]:
        return [
            self.owner.get_job('satellite_query', satellite)
            for satellite in self.source.dependent_satellites
        ]

    @property
    def csv_path(self) -> str:
        """
        :return: Returns the path to the supplied `Source` CSV file.
        """
        return self.owner.compute_service.csv_file_path(self.source.name)
