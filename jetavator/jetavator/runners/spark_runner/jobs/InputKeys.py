from __future__ import annotations

from typing import List
from functools import reduce

from jetavator.schema_registry import Satellite, SatelliteOwner

from pyspark.sql import functions as f, DataFrame

from .. import SparkView, SparkRunner, SparkJob


class InputKeys(SparkView, register_as='input_keys'):
    """
    Computes a DataFrame containing the `Hub` or `Link` key values for any
    satellite row that has been created, updated or deleted by one
    of this satellite's dependencies.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite:       The `Satellite` object that is receiving the
                            updated keys.
    :param satellite_owner: A `Hub` or `Link` describing the grain of the updated
                            keys. This does not necessarily have to be the same as
                            `satellite.parent` (or in the case of `Link`s, in
                            `satellite.parent.link_hubs`), as  dependent satellites
                            can have different data grain from this satellite.
    """

    checkpoint = False
    global_view = False

    def __init__(
            self,
            runner: SparkRunner,
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
            runner: SparkRunner,
            satellite: Satellite
    ) -> List[InputKeys]:
        """
        Generate an `InputKeys` job for any `Hub` or `Link`, if that Hub
        or Link can have keys generated for it by one of this satellite's
        eventual dependencies.

        :param runner:    The `SparkRunner` that is creating these objects.
        :param satellite: The `Satellite` to search for output keys for.
        :return:          A list of `OutputKeys` jobs containing output keys
                          for all relevant `Hub`s and `Link`s.
        """
        return [
            cls(runner, satellite, satellite_owner)
            for satellite_owner in satellite.input_keys()
        ]

    def execute_view(self) -> DataFrame:
        keys = [self.satellite_owner.key_column_name]
        if self.satellite_owner.type == 'link':
            keys += [
                f'hub_{alias}_key'
                for alias in self.satellite_owner.link_hubs.keys()
            ]

        key_tables = [
            self.spark.table(job.name)
            for job in self.dependencies
        ]

        def combine_key_tables(
                left: DataFrame,
                right: DataFrame
        ) -> DataFrame:
            return (
                left.join(
                    right,
                    left[keys[0]] == right[keys[0]],
                    how='full'
                ).select(
                    *[
                        f.coalesce(left[key], right[key]).alias(key)
                        for key in keys
                    ],
                    f.concat(
                        f.coalesce(left.key_source, f.array()),
                        f.coalesce(right.key_source, f.array())
                    ).alias('key_source')
                )
            )

        return reduce(combine_key_tables, key_tables)

    @property
    def dependencies(self) -> List[SparkJob]:
        return [
            self.runner.get_job('output_keys', dependent_satellite, self.satellite_owner)
            for dependent_satellite in self.dependent_satellites
        ]
