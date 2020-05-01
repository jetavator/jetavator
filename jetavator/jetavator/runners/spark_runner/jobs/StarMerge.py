from typing import List

from jetavator.schema_registry import SatelliteOwner

from .. import SparkJob, SparkRunner


class StarMerge(SparkJob, register_as='star_merge'):
    """
    Merges the created, updated or deleted rows into the the star schema
    Delta Lake table for this particular `Hub` or `Link`.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite_owner: The `Hub` or `Link` object that is being used to create
                            a Dimension or Fact table, respectively.
    """

    def __init__(
            self,
            runner: SparkRunner,
            satellite_owner: SatelliteOwner
    ) -> None:
        super().__init__(runner, satellite_owner)
        self.satellite_owner = satellite_owner

    @property
    def name(self) -> str:
        return f'merge_{self.satellite_owner.star_table_name}'

    def execute(self):
        path = [
            row
            for row in self.spark.sql(
                'DESCRIBE FORMATTED ' +
                self.satellite_owner.star_table_name
            ).collect()
            if row['col_name'] == 'Location'
        ][0]['data_type']

        source = self.spark.table(self.star_data_job.name)

        # Import has to happen inline because delta library is installed
        # at runtime by PySpark. Not ideal as not PEP8 compliant!
        # Create a setuptools-compatible mirror repo instead?

        # noinspection PyUnresolvedReferences
        from delta.tables import DeltaTable

        (
            DeltaTable
            .forPath(self.spark, path)
            .alias('target').merge(
                source.alias('source'),
                (
                    f'target.{self.satellite_owner.key_column_name}'
                    f' = source.{self.satellite_owner.key_column_name}'
                )
            )
            .whenMatchedDelete(condition='source.deleted_ind = 1')
            .whenMatchedUpdate(set={
                column: f"""
                    CASE WHEN array_contains(
                            source.key_source,
                            '{satellite.full_name}'
                         )
                         THEN source.{column}
                         ELSE target.{column}
                         END
                    """
                for satellite in self.satellite_owner.star_satellites.values()
                for column in satellite.columns.keys()
            })
            .whenNotMatchedInsertAll()
            .execute()
        )

    @property
    def star_data_job(self) -> SparkJob:
        """
        :return: The `StarData` job that contains the updated keys and data.
        """
        return self.runner.get_job('star_data', self.satellite_owner)

    @property
    def dependencies(self) -> List[SparkJob]:
        return [self.star_data_job]
