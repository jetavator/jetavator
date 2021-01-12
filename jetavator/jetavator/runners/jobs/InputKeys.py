from __future__ import annotations

from abc import ABC
from typing import List

from jetavator.schema_registry import Satellite, SatelliteOwner


from .. import Job, Runner


class InputKeys(Job, ABC, register_as='input_keys'):
    """
    Computes a DataFrame containing the `Hub` or `Link` key values for any
    satellite row that has been created, updated or deleted by one
    of this satellite's dependencies.

    :param runner:          The `Runner` that created this object.
    :param satellite:       The `Satellite` object that is receiving the
                            updated keys.
    :param satellite_owner: A `Hub` or `Link` describing the grain of the updated
                            keys. This does not necessarily have to be the same as
                            `satellite.parent` (or in the case of `Link`s, in
                            `satellite.parent.hubs`), as  dependent satellites
                            can have different data grain from this satellite.
    """

    def __init__(
            self,
            runner: Runner,
            satellite: Satellite,
            satellite_owner: SatelliteOwner
    ) -> None:
        super().__init__(runner, satellite, satellite_owner)
        self.satellite = satellite
        self.satellite_owner = satellite_owner

    @property
    def name(self) -> str:
        return (
            'vault_updates'
            f'_{self.satellite_owner.full_name}'
            f'_{self.satellite.full_name}'
        )

    @property
    def dependent_satellites(self) -> List[Satellite]:
        return self.satellite.dependent_satellites_by_owner(
            self.satellite_owner)

    @classmethod
    def keys_for_satellite(
            cls,
            runner: Runner,
            satellite: Satellite
    ) -> List[InputKeys]:
        """
        Generate an `InputKeys` job for any `Hub` or `Link`, if that Hub
        or Link can have keys generated for it by one of this satellite's
        eventual dependencies.

        :param runner:    The `Runner` that is creating these objects.
        :param satellite: The `Satellite` to search for output keys for.
        :return:          A list of `OutputKeys` jobs containing output keys
                          for all relevant `Hub`s and `Link`s.
        """
        return [
            cls(runner, satellite, satellite_owner)
            for satellite_owner in satellite.input_keys
        ]

    @property
    def dependencies(self) -> List[Job]:
        return [
            self.runner.get_job('output_keys', dependent_satellite, self.satellite_owner)
            for dependent_satellite in self.dependent_satellites
        ]
