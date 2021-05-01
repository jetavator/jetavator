import os

import wysdom

from shutil import copyfile

from jetavator.config import ComputeServiceConfig, ConfigProperty
from .SparkService import SparkService


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
