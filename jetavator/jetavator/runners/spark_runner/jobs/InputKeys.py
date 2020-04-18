from __future__ import annotations

from typing import List
from functools import reduce

from jetavator import KeyType
from jetavator.schema_registry import Satellite, SatelliteOwner

from pyspark.sql import functions as f, DataFrame

from .. import SparkView, SparkRunnerABC


class InputKeys(SparkView, register_as='input_keys'):
    checkpoint = False
    global_view = False

    def __init__(
        self,
        runner: SparkRunnerABC,
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
            self.satellite_owner.key)

    @classmethod
    def keys_for_satellite(
        cls,
        runner: SparkRunnerABC,
        satellite: Satellite,
        key_type: KeyType
    ) -> List[InputKeys]:
        # TODO: Implement KeyType throughout schema_registry to avoid .value conversions
        if type(key_type) is str:
            key_type = KeyType.HUB if key_type == 'hub' else KeyType.LINK
        return [
            cls(runner, satellite, satellite_owner)
            for satellite_owner in satellite.input_keys(key_type.value).values()
        ]

    def execute_view(self) -> DataFrame:
        keys = [self.satellite_owner.key_column_name]
        if self.satellite_owner.type == 'link':
            keys += [
                f'hub_{alias}_key'
                for alias in self.satellite_owner.link_hubs.keys()
            ]

        key_tables = [
            self.spark.table(
                f'keys_{self.satellite_owner.full_name}'
                f'_{dependent_satellite.full_name}'
            )
            for dependent_satellite in self.dependent_satellites
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

    def satellite_owner_output_keys(
        self,
        satellite: Satellite
    ) -> str:
        owner = self.satellite_owner
        if owner.name not in satellite.input_keys(owner.type):
            job_class = 'output_keys_from_satellite'
        elif owner.name not in satellite.produced_keys(owner.type):
            job_class = 'output_keys_from_dependencies'
        else:
            job_class = 'output_keys_from_both'
        return self.construct_job_key(job_class, satellite, owner)

    @property
    def dependency_keys(self) -> List[str]:
        return [
            self.satellite_owner_output_keys(dependent_satellite)
            for dependent_satellite in self.dependent_satellites
        ]
