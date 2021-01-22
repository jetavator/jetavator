from abc import ABC, abstractmethod
from typing import Iterable, Any, Dict

import sqlalchemy
import sqlalchemy_views
import pandas
import sqlparse

from jetavator import EngineABC
from jetavator.config import StorageServiceConfig
from .Service import Service
from .ComputeOwnedService import ComputeOwnedService
from .StorageServiceABC import StorageServiceABC
from .ExecutesSQL import ExecutesSQL


class StorageService(
    ComputeOwnedService,
    Service[StorageServiceConfig],
    ExecutesSQL,
    StorageServiceABC,
    ABC
):

    @property
    def engine(self) -> EngineABC:
        return self.owner.engine

    def create_schema_if_missing(self) -> None:
        if self.schema_exists:
            if self.config.drop_schema_if_exists:
                self.logger.info('Dropping and recreating database')
                self.drop_schema()
                self.create_schema()
            elif (
                    not self.schema_empty
                    and not self.engine.config.skip_deploy
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
            sqlalchemy_views: Iterable[sqlalchemy_views.CreateView]
    ) -> None:
        for view in sqlalchemy_views:
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

    @abstractmethod
    def load_dataframe(
            self,
            dataframe: pandas.DataFrame,
            source_name: str,
            source_column_names: Iterable[str]
    ) -> None:
        pass

    @property
    @abstractmethod
    def sqlalchemy_dialect(self) -> sqlalchemy.engine.interfaces.Dialect:
        pass

    @staticmethod
    def compile_sqlalchemy_with_dialect(
            sqlalchemy_element: sqlalchemy.sql.expression.ClauseElement,
            dialect: sqlalchemy.engine.interfaces.Dialect
    ) -> str:
        def compile_with_kwargs(**compile_kwargs: Any) -> str:
            return sqlparse.format(
                    str(sqlalchemy_element.compile(
                        dialect=dialect,
                        compile_kwargs=compile_kwargs
                    )),
                    reindent=True,
                    keyword_case='upper'
                )
        try:
            return compile_with_kwargs(literal_binds=True)
        except TypeError:
            return compile_with_kwargs()

    def compile_sqlalchemy(
            self,
            sqlalchemy_element: sqlalchemy.sql.expression.ClauseElement
    ) -> str:
        return self.compile_sqlalchemy_with_dialect(
            sqlalchemy_element,
            self.sqlalchemy_dialect)

    @abstractmethod
    def merge_from_spark_view(
            self,
            storage_table_name: str,
            spark_view_name: str,
            key_column_name: str,
            column_names: Iterable[str],
            column_references: Dict[str, str]
    ):
        pass
