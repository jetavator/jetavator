from functools import reduce
from typing import List

from jetavator.schema_registry import Satellite, SatelliteOwner

from pyspark.sql import functions as f, DataFrame

from .. import SparkJobABC, SparkView, SparkRunnerABC


class InputKeys(SparkView, register_as='input_keys'):
    name_template = (
        'vault_updates'
        '_{{satellite_owner.full_name}}'
        '_{{satellite.full_name}}'
    )
    template_args = ['satellite', 'satellite_owner', 'dependent_satellites']
    key_args = ['satellite', 'satellite_owner']
    checkpoint = False
    global_view = False

    def __init__(
        self,
        runner: SparkRunnerABC,
        satellite: Satellite,
        satellite_owner: SatelliteOwner
    ) -> None:
        super().__init__(
            runner,
            satellite,
            satellite_owner,
            dependent_satellites=satellite.dependent_satellites_by_owner(
                satellite_owner.key)
        )
        self.satellite = satellite
        self.satellite_owner = satellite_owner
        self.dependent_satellites = satellite.dependent_satellites_by_owner(
            satellite_owner.key)

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

    @property
    def dependencies(self) -> List[SparkJobABC]:
        return [
            self.runner.satellite_owner_output_keys(
                dependent_satellite,
                self.satellite_owner,
                self.satellite_owner.type
            )
            for dependent_satellite in self.dependent_satellites
        ]
