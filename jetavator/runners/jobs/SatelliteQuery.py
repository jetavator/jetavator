from abc import ABC
from typing import List

from jetavator.schema_registry import Satellite

from .. import Job, Runner


class SatelliteQuery(Job, ABC, register_as='satellite_query'):
    """
    Computes a DataFrame containing the result of the user query defined in
    `Satellite.pipeline` for a particular `Satellite`.

    :param runner:          The `Runner` that created this object.
    :param satellite:       The `Satellite` object containing the query definition.
    """

    def __init__(
            self,
            runner: Runner,
            satellite: Satellite
    ) -> None:
        super().__init__(
            runner,
            satellite
        )
        self.satellite = satellite

    @property
    def name(self) -> str:
        return f'vault_updates_{self.satellite.full_name}'

    @property
    def dependencies(self) -> List[Job]:
        return [
            *[
                self.owner.get_job('input_keys', self.satellite, satellite_owner)
                for satellite_owner in self.satellite.input_keys
            ],
            *[
                self.owner.get_job('serialise_satellite', dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'satellite'
                and dep.view in ['current', 'history']
            ],
            *[
                self.owner.get_job('create_source', dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'source'
            ]
        ]
