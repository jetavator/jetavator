from abc import ABC, abstractmethod
from typing import Iterable, Any, Dict, Optional, Set

import sqlalchemy
import sqlalchemy_views
import pandas

from jetavator.config import StorageServiceConfig
from jetavator.sql import ExecutesSQL, MetastoreInterface

from .Service import Service
from .ServiceOwner import ServiceOwner


class StorageService(
    Service[StorageServiceConfig, ServiceOwner],
    ExecutesSQL,
    MetastoreInterface,
    ABC
):

    def create_schema_if_missing(self) -> None:
        if self.schema_exists:
            if self.config.drop_schema_if_exists:
                self.logger.info('Dropping and recreating database')
                self.drop_schema()
                self.create_schema()
            elif (
                    not self.schema_empty
                    and not self.config.skip_deploy
            ):
                raise Exception(
                    f"Database {self.config.schema} already exists, "
                    "is not empty, and config.drop_schema_if_exists "
                    "is set to False."
                )
        else:
            self.logger.info(f'Creating database {self.config.schema}')
            self.create_schema()

    def create_table(self, sqlalchemy_table: sqlalchemy.schema.CreateTable) -> None:
        self.execute_sql_element(sqlalchemy_table)

    def create_tables(
            self,
            sqlalchemy_tables: Iterable[sqlalchemy.schema.CreateTable]
    ) -> None:
        for table in sqlalchemy_tables:
            self.create_table(table)

    def create_view(self, sqlalchemy_view: sqlalchemy_views.CreateView) -> None:
        self.execute_sql_element(sqlalchemy_view)

    def create_views(
            self,
            views: Iterable[sqlalchemy_views.CreateView]
    ) -> None:
        for view in views:
            self.create_view(view)

    def execute_sql_elements_async(
            self,
            sql_elements: Iterable[sqlalchemy.sql.expression.ClauseElement]
    ) -> None:
        # TODO: Implement async execution
        for element in sql_elements:
            self.execute_sql_element(element)

    @abstractmethod
    def sql_query_single_value(self, sql: str) -> Any:
        pass

    def execute_sql_element(
            self,
            sqlalchemy_element: sqlalchemy.sql.expression.ClauseElement,
            async_cursor: bool = False
    ) -> pandas.DataFrame:
        # TODO: Implement or deprecate async_cursor
        return self.execute(self.compile_sqlalchemy(sqlalchemy_element))

    @abstractmethod
    def test(self) -> None:
        pass

    @property
    def index_option_kwargs(self) -> Set[str]:
        return set()

    @abstractmethod
    def merge_from_spark_view(
            self,
            storage_table_name: str,
            spark_view_name: str,
            key_column_name: str,
            column_names: Iterable[str],
            column_references: Dict[str, str],
            deleted_indicator: Optional[str] = None
    ):
        pass
