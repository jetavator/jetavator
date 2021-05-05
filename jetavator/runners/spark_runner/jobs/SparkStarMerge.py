from pyspark.sql import DataFrame

from .. import SparkJob
from jetavator.runners.jobs import StarMerge


class SparkStarMerge(SparkJob, StarMerge, register_as='star_merge'):

    def execute(self) -> DataFrame:
        return self.owner.compute_service.star_storage_service.merge_from_spark_view(
            storage_table_name=self.satellite_owner.star_table_name,
            spark_view_name=self.star_data_job.name,
            key_column_name=self.satellite_owner.key_column_name,
            column_names=[
                self.satellite_owner.key_column_name,
                *(x.name for x in self.satellite_owner.link_key_columns),
                *self.star_column_references.keys()
            ],
            column_references=self.star_column_references
        )

