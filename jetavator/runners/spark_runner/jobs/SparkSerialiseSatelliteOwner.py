from pyspark.sql import DataFrame

from .. import SparkJob
from jetavator.runners.jobs import SerialiseSatelliteOwner


class SparkSerialiseSatelliteOwner(
    SparkJob,
    SerialiseSatelliteOwner,
    register_as='serialise_satellite_owner'
):

    def execute(self) -> DataFrame:
        return self.owner.compute_service.vault_storage_service.merge_from_spark_view(
            storage_table_name=self.satellite_owner.table_name,
            spark_view_name=self.satellite_owner_keys_job.name,
            key_column_name=self.satellite_owner.key_column_name,
            column_names=[
                self.satellite_owner.key_column_name,
                *(x.name for x in self.satellite_owner.link_key_columns),
                f"{self.satellite_owner.type}_load_dt",
                f"{self.satellite_owner.type}_record_source"
            ],
            column_references={}
        )



