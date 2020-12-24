from typing import Any

from abc import ABC, abstractmethod

import pandas
import sqlalchemy

from .Service import Service


class DBService(Service, ABC):

    @abstractmethod
    def metadata(self) -> sqlalchemy.MetaData:
        pass

    @abstractmethod
    def execute(self, sql) -> pandas.DataFrame:
        pass

    @abstractmethod
    def drop_schema(self) -> None:
        pass

    @abstractmethod
    def create_schema(self) -> None:
        pass

    @property
    @abstractmethod
    def schema_empty(self) -> bool:
        pass

    @property
    @abstractmethod
    def schema_exists(self) -> bool:
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        pass

    @abstractmethod
    def column_exists(self, table_name: str, column_name: str) -> bool:
        pass

    @abstractmethod
    def sql_query_single_value(self, sql: str) -> Any:
        pass

    @abstractmethod
    def execute_sql_element(self, sql_element, async_cursor=False):
        pass

    @abstractmethod
    def test(self) -> None:
        pass

    @abstractmethod
    def load_dataframe(self, dataframe, source):
        pass

    @abstractmethod
    def create_tables(self, sqlalchemy_tables):
        pass

    @abstractmethod
    def execute_sql_elements_async(self, sql_elements) -> None:
        pass

    @staticmethod
    def sql_script_filename(ddl_element: sqlalchemy.schema.DDLElement) -> str:
        """
        sqlalchemy_ddl_element: sqlalchemy.sql.ddl.DDLElement
        """
        ddl_statement_type = type(ddl_element).__visit_name__

        name = getattr(
            ddl_element.element, "name", str(ddl_element.element))
        schema = getattr(
            ddl_element.element, "schema", None)

        if schema:
            qualified_name = f"{schema}.{name}"
        else:
            qualified_name = name

        return f"{ddl_statement_type}/{qualified_name}"
