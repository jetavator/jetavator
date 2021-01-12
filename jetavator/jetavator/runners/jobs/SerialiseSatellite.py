from abc import ABC
from typing import List

from jetavator.schema_registry import Satellite

from .. import Job, Runner


class SerialiseSatellite(Job, ABC, register_as='serialise_satellite'):
    """
    Serialises the created, updated or deleted rows for a `Satellite` to its
    Delta Lake table.

    :param runner:          The `Runner` that created this object.
    :param satellite:       The `Satellite` object containing the query definition.
    """

    def __init__(self, runner: Runner, satellite: Satellite) -> None:
        super().__init__(runner, satellite)
        self.satellite = satellite

    @property
    def name(self) -> str:
        return f'serialise_sat_{self.satellite.name}'

    @property
    def satellite_query_job(self) -> Job:
        """
        :return: The `SatelliteQuery` job that contains the updated data.
        """
        return self.runner.get_job('satellite_query', self.satellite)

    @property
    def dependencies(self) -> List[Job]:
        return [self.satellite_query_job]
