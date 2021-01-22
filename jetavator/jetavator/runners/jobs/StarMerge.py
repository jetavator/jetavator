from abc import ABC
from typing import List, Dict

from jetavator.schema_registry import SatelliteOwner, Satellite

from .. import Job, Runner


class StarMerge(Job, ABC, register_as='star_merge'):
    """
    Merges the created, updated or deleted rows into the the star schema
    Delta Lake table for this particular `Hub` or `Link`.

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
        return f'merge_{self.satellite_owner.star_table_name}'

    @property
    def star_data_job(self) -> Job:
        """
        :return: The `StarData` job that contains the updated keys and data.
        """
        return self.runner.get_job('star_data', self.satellite_owner)

    @property
    def star_column_references(self) -> Dict[str, Satellite]:
        """
        :return: An iterator of tuples containing column names in the star schema
                 and their owning satellites.
        """
        return {
            column: satellite.name
            for satellite in self.satellite_owner.star_satellites.values()
            for column in satellite.columns.keys()
        }

    @property
    def dependencies(self) -> List[Job]:
        return [self.star_data_job]
