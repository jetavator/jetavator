import os
from abc import ABC

import wysdom

from shutil import copyfile
from lazy_property import LazyProperty
from pyspark.sql import SparkSession

from jetavator.config import ComputeServiceConfig, ConfigProperty
from .SparkService import SparkService, SPARK_APP_NAME, DELTA_VERSION


class LocalSparkConfig(ComputeServiceConfig):
    type: str = ConfigProperty(wysdom.SchemaConst('local_spark'))


class LocalSparkService(SparkService, register_as="local_spark"):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tempfolder = '/jetavator/data'
        # Figure out a better way to manage temporary folders -
        # requires storage of state between commands line calls!

    # TODO: Remove SparkService.session
    def session(self):
        raise NotImplementedError

    @LazyProperty
    def spark(self):
        os.environ['PYSPARK_SUBMIT_ARGS'] = (
            '--packages'
            f' io.delta:{DELTA_VERSION}'
            ' pyspark-shell'
        )
        spark_session = (
            SparkSession
            .builder
            .appName(SPARK_APP_NAME)
            .enableHiveSupport()
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
        spark_session.sparkContext.setLogLevel('ERROR')
        return spark_session

    def csv_file_path(self, source_name: str):
        return (
            f'{self.tempfolder}/'
            f'{self.config.schema}/'
            f'{self.owner.config.session.run_uuid}/'
            f'{source_name}.csv'
        )

    def source_csv_exists(self, source_name: str) -> bool:
        return os.path.exists(self.csv_file_path(source_name))

    def load_csv(self, csv_file, source_name: str):
        self.logger.info(f"{source_name}.csv: Uploading file")
        try:
            os.makedirs(
                os.path.dirname(
                    self.csv_file_path(source_name)))
        except FileExistsError:
            pass
        copyfile(csv_file, self.csv_file_path(source_name))
