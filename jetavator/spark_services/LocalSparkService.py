import os

import wysdom

from jetavator.config import ConfigProperty

from .SparkService import SparkConfig, SparkService


class LocalSparkConfig(SparkConfig):
    type: str = ConfigProperty(wysdom.SchemaConst('spark'))


class LocalSparkService(SparkService, register_as="spark"):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tempfolder = '/jetavator/data'
        # TODO: Figure out a better way to manage temporary folders -
        #       requires storage of state between command line calls!

    def csv_file_path(self, source_name: str):
        return (
            f'{self.tempfolder}/'
            f'{self.config.schema}/'
            f'{self.config.runner.session.run_uuid}/'
            f'{source_name}.csv'
        )

    def source_csv_exists(self, source_name: str) -> bool:
        return os.path.exists(self.csv_file_path(source_name))
