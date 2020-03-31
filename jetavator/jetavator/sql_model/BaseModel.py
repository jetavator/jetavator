from sqlalchemy import Table, text, select
from sqlalchemy.sql.expression import literal_column
from sqlalchemy.schema import CreateTable, DropTable, CreateIndex
from sqlalchemy_views import CreateView, DropView
from ..schema_registry.sqlalchemy_tables import (
    Log)

from jetavator.mixins import RegistersSubclasses


class HasSQLModel(object):

    def __init_subclass__(cls, sql_model_class=None, **kwargs):
        cls.sql_model_class = sql_model_class
        super().__init_subclass__(**kwargs)

    @property
    def sql_model(self):
        sql_model_class = self.sql_model_class or self.registered_name
        if sql_model_class in BaseModel.registered_subclasses():
            return BaseModel.registered_subclass_instance(
                sql_model_class, self)


class BaseModel(RegistersSubclasses):

    def __init__(self, definition):
        self.type = self.registered_name
        self.definition = definition

    @property
    def schema(self):
        return self.project.registry.config.schema

    @property
    def metadata(self):
        return self.project.connection.metadata

    @property
    def project(self):
        return getattr(self.definition, "project", self.definition)

    def define_table(self, name, *args, **kwargs):
        table_name = (
            f"{kwargs['schema']}.{name}"
            if 'schema' in kwargs
            else name
        )
        if table_name in self.metadata.tables:
            return self.metadata.tables[table_name]
        else:
            return Table(
                name, self.metadata, *args, **kwargs
            )

    def columns_in_table(self, table, columns):
        return [
            table.columns[column.name]
            for column in columns
        ]

    def create_or_drop_view(self, view, view_query):
        if self.definition.action == "create":
            return CreateView(view, view_query)
        elif self.definition.action == "drop":
            return DropView(view)

    def create_or_alter_table(self, table, with_index=False):
        files = []
        # we need "none" because "action" doesn't yet pick up if satellites
        # have changed, but if they have some tables need to be recreated
        if self.definition.action in ("alter", "drop", "none"):
            files += [DropTable(table)]
        if self.definition.action in ("alter", "create", "none"):
            files += self.create_table(table, with_index)
        return files

    def create_or_alter_tables(self, tables, with_index=False):
        return [
            statement
            for table in tables
            for statement in self.create_or_alter_table(table, with_index)
        ]

    def create_table(self, table, with_index=False):
        statements = [CreateTable(table)]
        if with_index:
            statements += [
                CreateIndex(index)
                for index in table.indexes
            ]
        return statements

    def create_tables(self, tables, with_index=False):
        return [
            statement
            for table in tables
            for statement in self.create_table(table, with_index)
        ]

    def drop_tables(self, tables):
        return [
            DropTable(table)
            for table in tables
        ]

    def copy_rows(self, table_or_query_from, table_to):
        return table_to.insert().from_select(
            table_to.c,
            select([
                table_or_query_from.c[column.name]
                for column in table_to.c
            ])
        )
