from __future__ import annotations

from functools import reduce
from pyspark.sql import functions as f, DataFrame

from .. import SparkView
from jetavator.runners.jobs import InputKeys


class SparkInputKeys(SparkView, InputKeys, register_as='input_keys'):

    checkpoint = False
    global_view = False

    def execute_view(self) -> DataFrame:
        keys = [self.satellite_owner.key_column_name]
        if self.satellite_owner.type == 'link':
            keys += [
                f'hub_{alias}_key'
                for alias in self.satellite_owner.hubs.keys()
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

