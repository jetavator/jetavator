from __future__ import annotations

from typing import List

from abc import ABC

from jetavator.schema_registry import Satellite, SatelliteOwner, HubKeyColumn

from .. import Job, Runner


class ProducedKeys(Job, ABC, register_as='produced_keys'):
    """
    Computes a DataFrame containing the `Hub` or `Link` key values for any
    satellite row that has been created, updated or deleted by this satellite
    or its dependencies.

    :param runner:          The `Runner` that created this object.
    :param satellite:       The `Satellite` object that is passing the updated
                            keys to its dependencies.
    :param satellite_owner: A `Hub` or `Link` describing the grain of the updated
                            keys.
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

    @classmethod
    def keys_for_satellite(
            cls,
            owner: Runner,
            satellite: Satellite
    ) -> List[ProducedKeys]:
        """
        Generate an `InputKeys` job for any `Hub` or `Link`, if that Hub
        or Link can have keys generated for it by this satellite.

        :param owner:    The `Runner` that is creating these objects.
        :param satellite: The `Satellite` to search for output keys for.
        :return:          A list of `OutputKeys` jobs containing output keys
                          for all relevant `Hub`s and `Link`s.
        """
        return [
            cls.registered_subclass_instance(
                f'produced_{satellite_owner.registered_name}_keys',
                owner,
                satellite,
                satellite_owner
            )
            for satellite_owner in satellite.produced_keys
        ]

    @property
    def name(self) -> str:
        return (
            'produced_keys'
            f'_{self.satellite_owner.full_name}'
            f'_{self.satellite.full_name}'
        )

    @property
    def satellite_query_job(self) -> Job:
        """
        :return: The `SatelliteQuery` job that contains the updated data.
        """
        return self.owner.get_job('satellite_query', self.satellite)

    @property
    def dependencies(self) -> List[Job]:
        return [self.satellite_query_job]


class ProducedLinkKeys(ProducedKeys, ABC, register_as='produced_link_keys'):
    """
    Computes a DataFrame containing the `Link` key values for any
    satellite row that has been created, updated or deleted by this satellite
    or its dependencies.
    """
    pass


class ProducedHubKeys(ProducedKeys, ABC, register_as='produced_hub_keys'):
    """
    Computes a DataFrame containing the `Hub` key values for any
    satellite row that has been created, updated or deleted by this satellite
    or its dependencies.
    """

    @property
    def key_columns(self) -> List[HubKeyColumn]:
        """
        :return: A list of the `HubKeyColumn`s for this particular `Hub`.
        """
        return self.satellite.hub_key_columns[self.satellite_owner.name]
