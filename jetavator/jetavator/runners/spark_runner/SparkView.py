from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from . import SparkJob, SparkSQLJob


class SparkView(SparkJob, ABC):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    @abstractmethod
    def checkpoint(self) -> bool:
        pass

    @property
    @abstractmethod
    def global_view(self) -> bool:
        pass

    @abstractmethod
    def execute_view(self) -> DataFrame:
        pass

    def execute(self) -> DataFrame:
        df = self.execute_view()

        if self.checkpoint:
            df = df.localCheckpoint()

        if self.global_view:
            df = df.createOrReplaceGlobalTempView(self.name)
        else:
            df = df.createOrReplaceTempView(self.name)

        return df


class SparkSQLView(SparkView, SparkSQLJob, ABC):

    def execute_view(self) -> DataFrame:
        return self.execute_sql()
