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
