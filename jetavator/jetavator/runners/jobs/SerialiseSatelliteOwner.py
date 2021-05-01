from abc import ABC
from typing import List

from jetavator.schema_registry import SatelliteOwner

from .. import Job, Runner


class SerialiseSatelliteOwner(Job, ABC, register_as='serialise_satellite_owner'):
    """
    Serialises the created, updated or deleted rows for a `Satellite` to its
    Delta Lake table.

    :param runner:          The `Runner` that created this object.
    :param satellite_owner: The `SatelliteOwner` to serialise.
    """

    def __init__(self, runner: Runner, satellite_owner: SatelliteOwner) -> None:
        super().__init__(runner, satellite_owner)
        self.satellite_owner = satellite_owner

    @property
    def name(self) -> str:
        return f'serialise_{self.satellite_owner.type}_{self.satellite_owner.name}'

    @property
    def satellite_owner_keys_job(self) -> Job:
        """
        :return: The `SatelliteOwnerKeys` job that contains the updated keys.
        """
        return self.runner.get_job('satellite_owner_keys', self.satellite_owner)

    @property
    def dependencies(self) -> List[Job]:
        return [self.satellite_owner_keys_job]
