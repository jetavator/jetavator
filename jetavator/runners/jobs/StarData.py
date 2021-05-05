from abc import ABC
from typing import List

from jetavator.schema_registry import SatelliteOwner

from .. import Job, Runner


class StarData(Job, ABC, register_as='star_data'):
    """
    Computes the created, updated or deleted rows for the star schema table
    for this particular `Hub` or `Link`.

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
        return f'updates_{self.satellite_owner.star_table_name}'

    @property
    def satellite_owner_keys_job(self) -> Job:
        """
        :return: The `SatelliteOwnerKeys` job that contains the updated keys.
        """
        return self.owner.get_job('satellite_owner_keys', self.satellite_owner)

    @property
    def satellite_query_jobs(self) -> List[Job]:
        """
        :return: A list of the `SatelliteQuery` jobs that contain the updated data.
        """
        return [
            self.owner.get_job('satellite_query', satellite)
            for satellite in self.satellite_owner.star_satellites.values()
        ]

    @property
    def dependencies(self) -> List[Job]:
        return [
            self.satellite_owner_keys_job,
            *self.satellite_query_jobs
        ]
