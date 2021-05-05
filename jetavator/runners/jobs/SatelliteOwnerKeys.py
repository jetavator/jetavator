from abc import ABC
from typing import List

from jetavator.schema_registry import SatelliteOwner

from .. import Job, Runner


class SatelliteOwnerKeys(Job, ABC, register_as='satellite_owner_keys'):
    """
    Collects all the key values for any `Satellite` row that has been created,
    updated or deleted for this particular `Hub` or `Link`.

    :param runner:          The `Runner` that created this object.
    :param satellite_owner: The `Hub` or `Link` object that is being used to create
                            a Dimension or Fact table, respectively.
    """

    def __init__(
            self,
            runner: Runner,
            satellite_owner: SatelliteOwner
    ) -> None:
        super().__init__(runner, satellite_owner)
        self.satellite_owner = satellite_owner

    @property
    def name(self) -> str:
        return f'keys_{self.satellite_owner.full_name}'

    @property
    def dependencies(self) -> List[Job]:
        return [
            self.owner.get_job('output_keys', satellite, self.satellite_owner)
            for satellite
            in self.satellite_owner.satellites_containing_keys.values()
        ]
