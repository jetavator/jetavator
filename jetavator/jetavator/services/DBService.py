from abc import ABC, abstractmethod

from logging import Logger

from sqlalchemy.schema import DDLElement

from .Service import Service


class DBService(Service, ABC):

    @abstractmethod
    def metadata(self):
        pass

    @abstractmethod
    def execute(self, sql):
        pass

    @abstractmethod
    def drop_schema(self):
        pass

    @abstractmethod
    def create_schema(self):
        pass

    @property
    @abstractmethod
    def schema_empty(self):
        pass

    @property
    @abstractmethod
    def schema_exists(self):
        pass

    @abstractmethod
    def table_exists(self, table_name):
        pass

    @abstractmethod
    def column_exists(self, table_name, column_name):
        pass

    @abstractmethod
    def sql_query_single_value(self, sql):
        pass

    @abstractmethod
    def execute_sql_element(self, sql_element, async_cursor=False):
        pass

    @abstractmethod
    def test(self):
        pass

    @abstractmethod
    def session(self):
        pass

    @property
    @abstractmethod
    def logger(self) -> Logger:
        pass

    @abstractmethod
    def load_dataframe(self, dataframe, source):
        pass

    @abstractmethod
    def write_empty_table(self, sqlalchemy_table, overwrite_schema=True):
        pass

    @abstractmethod
    def create_tables(self, sqlalchemy_tables):
        pass

    @abstractmethod
    def execute_sql_elements_async(self, sql_elements):
        pass

    @staticmethod
    def sql_script_filename(ddl_element: DDLElement) -> str:
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
