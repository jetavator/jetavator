from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from . import SparkJob, SparkSQLJob

LOG_ROW_COUNTS = True
SAMPLE_N_ROWS = 10


class SparkView(SparkJob, ABC):
    """
    Base class for all Spark jobs that are registered in the metastore
    as a temporary view after execution.
    """

    @property
    @abstractmethod
    def checkpoint(self) -> bool:
        """
        This property should be set to True if the a Spark local
        checkpoint should be created for the resulting DataFrame.
        """
        pass

    @property
    @abstractmethod
    def global_view(self) -> bool:
        """
        This property should be set to True if the temporary view
        should be registered as a global view in the metastore.
        """
        pass

    @abstractmethod
    def execute_view(self) -> DataFrame:
        """
        Construct the underlying Spark DataFrame for this view.
        """
        pass

    def execute(self) -> DataFrame:
        df = self.execute_view()

        if self.checkpoint:
            df = df.localCheckpoint()

        # TODO: Make LOG_ROW_COUNTS configurable
        if LOG_ROW_COUNTS:
            self.logger.info(f'Row count: {self.name} ({df.count()} rows)')
        # TODO: Make SAMPLE_N_ROWS configurable
        if SAMPLE_N_ROWS:
            df.limit(SAMPLE_N_ROWS).write.format("json").save(
                f".jetavator/debug_samples"
                f"/{self.runner.engine.config.session.run_uuid}/{self.name}.json")

        if self.global_view:
            df = df.createOrReplaceGlobalTempView(self.name)
        else:
            df = df.createOrReplaceTempView(self.name)

        return df


class SparkSQLView(SparkView, SparkSQLJob, ABC):
    """
    Base class for all Spark jobs that register a temporary view and
    generate declarative Spark SQL to produce that view.
    """

    def execute_view(self) -> DataFrame:
        return self.execute_sql()
