from abc import ABC, abstractmethod

import pandas
import pyspark

from jetavator.config import ConfigWithSchema
from jetavator.HasConfig import HasConfig

from jetavator.sql import ExecutesSQL


class ExecutesSparkSQL(HasConfig[ConfigWithSchema], ExecutesSQL, ABC):

    @property
    @abstractmethod
    def spark(self) -> pyspark.SQLContext:
        pass

    def execute(self, sql) -> pandas.DataFrame:
        self.spark.sql(f'USE `{self.config.schema}`')
        try:
            return self.spark.sql(sql).toPandas()
        except RuntimeError as e:
            raise Exception(
                f"""
                Config dump:
                {self.config}

                Error while trying to run script:
                {sql}
                """ + str(e)
            )
