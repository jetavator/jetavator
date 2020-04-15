from logging import Logger

from .Service import Service


class DBService(Service):

    def metadata(self):
        raise NotImplementedError

    def execute(self, sql):
        raise NotImplementedError

    def drop_schema(self):
        raise NotImplementedError

    def create_schema(self):
        raise NotImplementedError

    @property
    def schema_empty(self):
        raise NotImplementedError

    @property
    def schema_exists(self):
        raise NotImplementedError

    def table_exists(self, table_name):
        raise NotImplementedError

    def column_exists(self, table_name, column_name):
        raise NotImplementedError

    def sql_query_single_value(self, sql):
        raise NotImplementedError

    def execute_sql_element(self, sql_element, async_cursor=False):
        raise NotImplementedError

    def test(self):
        raise NotImplementedError

    def session(self):
        raise NotImplementedError

    @property
    def logger(self) -> Logger:
        raise NotImplementedError

    def load_dataframe(self, dataframe, source):
        raise NotImplementedError

    def write_empty_table(self, sqlalchemy_table, overwrite_schema=True):
        raise NotImplementedError

    def create_tables(self, sqlalchemy_tables):
        raise NotImplementedError

    def execute_sql_elements_async(self, sql_elements):
        raise NotImplementedError
