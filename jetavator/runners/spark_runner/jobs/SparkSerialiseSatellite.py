from pyspark.sql import DataFrame

from .. import SparkJob
from jetavator.runners.jobs import SerialiseSatellite


class SparkSerialiseSatellite(SparkJob, SerialiseSatellite, register_as='serialise_satellite'):

    def execute(self) -> DataFrame:
        return self.owner.compute_service.vault_storage_service.write_table(
            table_name=self.satellite.table_name,
            df=self.spark.table(self.satellite_query_job.name)
        )
