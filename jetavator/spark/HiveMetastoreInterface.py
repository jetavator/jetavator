from typing import List
from abc import ABC, abstractmethod

import pyspark
import pandas

from jetavator.HasLogger import HasLogger
from jetavator.HasConfig import HasConfig


class HiveMetastoreInterface(HasLogger, HasConfig, ABC):

    @property
    @abstractmethod
    def spark(self) -> pyspark.SQLContext:
        pass

    def drop_schema(self) -> None:
        self.logger.info(f"{self.__class__.__name__} - dropping schema {self.config.schema}")
        self.spark.sql(
            f'DROP DATABASE `{self.config.schema}` CASCADE'
        )

    def create_schema(self) -> None:
        self.logger.info(f"{self.__class__.__name__} - creating schema {self.config.schema}")
        self.spark.sql(
            f'CREATE DATABASE `{self.config.schema}`'
        )

    @property
    def schema_empty(self) -> bool:
        result = not any(self.table_names)
        self.logger.debug(f"{self.__class__.__name__} - checking if list of tables is empty: {result}")
        return result

    @property
    def schema_exists(self) -> bool:
        result = str(self.config.schema) in self.schema_names
        self.logger.debug(f"{self.__class__.__name__} - checking if schema {self.config.schema} exists: {result}")
        return result

    def table_exists(self, table_name: str) -> bool:
        result = table_name in self.table_names
        self.logger.debug(f"{self.__class__.__name__} - checking if table {table_name} exists: {result}")
        return result

    def column_exists(self, table_name: str, column_name: str) -> bool:
        return column_name in self.column_names_in_table(table_name)

    @property
    def table_names(self) -> List[str]:
        self.logger.debug(f"{self.__class__.__name__} - listing tables in {self.config.schema}")
        return self._column_in_spark_query(
            select_column="tableName",
            from_query=f"SHOW TABLES IN `{self.config.schema}`"
        )

    @property
    def schema_names(self) -> List[str]:
        self.logger.debug(f"{self.__class__.__name__} - listing schemas")
        return self._column_in_spark_query(
            select_column="namespace",
            from_query="SHOW DATABASES"
        )

    def column_names_in_table(self, table_name: str) -> List[str]:
        self.logger.debug(f"{self.__class__.__name__} - listing columns in {table_name}")
        return self._column_in_spark_query(
            select_column="col_name",
            from_query=f"DESCRIBE FORMATTED `{self.config.schema}`.`{table_name}`"
        )

    def _column_in_spark_query(self, select_column: str, from_query: str) -> List[str]:
        return [
            row[select_column]
            for row in self.spark.sql(from_query).collect()
        ]
