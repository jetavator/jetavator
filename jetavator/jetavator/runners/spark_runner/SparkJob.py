from __future__ import annotations
from abc import ABC, abstractmethod

import jinja2

from pyspark.sql import DataFrame, SparkSession

from .. import Job

COALESCE_PARTITIONS = 10


class SparkJob(Job, ABC):

    @property
    def spark(self) -> SparkSession:
        """
        The Spark session that this SparkJob will use for execution.
        """
        return self.runner.compute_service.spark

    @abstractmethod
    def execute(self) -> DataFrame:
        """
        Execute the underlying Spark job and return the resultant `DataFrame`.
        """
        pass


class SparkSQLJob(SparkJob, ABC):
    """
    Base class for all Spark jobs that use a template to generate
    declarative Spark SQL.
    """

    @property
    @abstractmethod
    def sql_template(self) -> str:
        """
        A Jinja2-compatible template that returns valid Spark SQL.
        Members of this class or subclasses may be accessed in the
        template through 'job._' which is a synonym for 'self._'.
        """
        pass

    @property
    def query(self) -> str:
        """
        The Spark SQL statement to be executed by this SparkSQLJob.
        """
        return jinja2.Template(self.sql_template).render(job=self)

    def execute_sql(self) -> DataFrame:
        """
        Execute the Spark SQL statement and return the resulting `DataFrame`.
        """
        try:
            self.logger.debug(self.query)
            return self.spark.sql(self.query).coalesce(COALESCE_PARTITIONS)
        except Exception as e:
            raise Exception(f'''
                Job completed with error:
                {str(e)}

                Job script:
                {self.query}

                Dependencies:
                {self.dependencies}
            ''')

    def execute(self) -> DataFrame:
        return self.execute_sql()
