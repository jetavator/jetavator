import sqlalchemy
import lazy_property

from .DBService import DBService


class MSSQLService(DBService, register_as='mssql'):

    @lazy_property.LazyProperty
    def sqlalchemy_connection(self):
        if self.config.trusted_connection:
            return sqlalchemy.create_engine(
                "mssql+pyodbc://{server}:1433/{database}"
                "?driver=ODBC+Driver+17+for+SQL+Server".format(
                    server=self.config.server,
                    database=self.config.database
                ),
                connect_args={'autocommit': True},
                deprecate_large_types=True
            )
        else:
            return sqlalchemy.create_engine(
                "mssql+pyodbc://{username}:{password}@{server}:1433/{database}"
                "?driver=ODBC+Driver+17+for+SQL+Server".format(
                    username=self.config.username,
                    password=self.config.password,
                    server=self.config.server,
                    database=self.config.database
                ),
                connect_args={'autocommit': True},
                deprecate_large_types=True
            )

    @lazy_property.LazyProperty
    def metadata(self):
        meta = sqlalchemy.MetaData()
        meta.bind = self.sqlalchemy_connection
        return meta

    def execute(self, sql):
       for sql_statement in sql.encode(
            "ascii", "ignore"
        ).decode("ascii").split("GO\n"):
            try:
                self.sqlalchemy_connection.execute(
                    sql_statement
                )
            except (
                sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.DBAPIError
            ) as e:
                raise Exception(
                    f"""
                    Config dump:
                    {self.config}

                    Error while strying to run script:
                    {sql_statement}
                    """ + str(e)
                    )


    def drop_schema(self):
        self.sqlalchemy_connection.execute(
            f"""
            DECLARE @drop_statements AS CURSOR
            DECLARE @statement AS VARCHAR(max)

            SET @drop_statements = CURSOR FOR
            SELECT 'DROP TABLE [{self.config.schema}].[' + TABLE_NAME + ']'
              FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_SCHEMA = '{self.config.schema}'

            OPEN @drop_statements

            FETCH NEXT FROM @drop_statements INTO @statement
            WHILE @@FETCH_STATUS = 0
            BEGIN
             EXECUTE (@statement)
             FETCH NEXT FROM @drop_statements INTO @statement
            END

            CLOSE @drop_statements
            DEALLOCATE @drop_statements
            """
        )
        self.sqlalchemy_connection.execute(
            f"DROP SCHEMA [{self.config.schema}]"
        )

    def create_schema(self):
        self.sqlalchemy_connection.execute(
            "CREATE SCHEMA [" + self.config.schema + "]"
        )

    @property
    def schema_empty(self):
        return (
            len(
                self.sqlalchemy_connection.execute(
                    f"""
                    SELECT TOP 1
                           TABLE_NAME
                      FROM INFORMATION_SCHEMA.TABLES
                     WHERE TABLE_CATALOG = '{self.config.database}'
                       AND TABLE_SCHEMA = '{self.config.schema}'
                    """
                ).fetchall()
            ) == 0
        )

    @property
    def schema_exists(self):
        return self._sql_exists(
            f"""
            SELECT SCHEMA_NAME
              FROM INFORMATION_SCHEMA.SCHEMATA
             WHERE CATALOG_NAME = '{self.config.database}'
               AND SCHEMA_NAME = '{self.config.schema}'
            """
        )

    def _sql_exists(self, sql):
        result_proxy = self.sqlalchemy_connection.execute(sql)
        return bool(result_proxy.first())

    def table_exists(self, table_name):
        return self._sql_exists(
            f"""
            SELECT TABLE_NAME
              FROM INFORMATION_SCHEMA.TABLES
             WHERE TABLE_CATALOG = '{self.config.database}'
               AND TABLE_SCHEMA = '{self.config.schema}'
               AND TABLE_NAME = '{table_name}'
            """
        )

    def column_exists(self, table_name, column_name):
        return self._sql_exists(
            f"""
            SELECT COLUMN_NAME
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_CATALOG = '{self.config.database}'
               AND TABLE_SCHEMA = '{self.config.schema}'
               AND TABLE_NAME = '{table_name}'
               AND COLUMN_NAME = '{column_name}'
            """
        )

    def sql_query_single_value(self, sql):
        try:
            return self.sqlalchemy_connection.execute(
                sql
            ).first()[0]
        except TypeError:
            return None

    def execute_sql_element(self, sql_element, async_cursor=False):
        return self.sqlalchemy_connection.execute(sql_element).fetchall()
