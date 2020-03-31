from .BaseModel import BaseModel

from .functions import hash_record

from sqlalchemy import Column, PrimaryKeyConstraint

from sqlalchemy.types import *
# from sqlalchemy.dialects.mssql import BIT


class SourceModel(BaseModel, register_as="source"):

    @property
    def files(self):
        if self.definition.action == "create":
            return self.create_tables(self.tables)
        else:
            return []

    @property
    def tables(self):
        return [
            self.table,
            self.history_table,
            self.error_table
        ]

    def source_columns(self, use_primary_key=True):
        return [
            Column(
                column_name,
                eval(column.type.upper().replace("MAX", "None")),
                nullable=True,
                primary_key=(use_primary_key and column.pk)
            )
            for column_name, column in self.definition.columns.items()
        ]

    def date_columns(self, use_primary_key=True):
        return [
            Column(
                "jetavator_load_dt",
                DATETIME,
                nullable=True,
                primary_key=use_primary_key),
            Column(
                "jetavator_deleted_ind",
                CHAR(1),
                nullable=True,
                default=0)
        ]

    def table_columns(self, include_primary_key=True):
        has_primary_key = bool(self.definition.primary_key_columns)
        # use_primary_key = has_primary_key and include_primary_key
        use_primary_key = False  # Databricks does not allow PKs. Make this configurable per engine?
        return [
            *self.source_columns(use_primary_key),
            *self.date_columns(use_primary_key)
        ]

    @property
    def table(self):
        return self.define_table(
            f"source_{self.definition.name}",
            *self.table_columns()
        )

    @property
    def history_table(self):
        return self.define_table(
            f"source_history_{self.definition.name}",
            *self.table_columns(),
            schema=self.schema
        )

    @property
    def error_table(self):
        return self.define_table(
            f"source_error_{self.definition.name}",
            *self.table_columns(include_primary_key=False),
            schema=self.schema
        )

    @property
    def delete_all(self):
        return [
            self.table.delete(),
            self.history_table.delete(),
            self.error_table.delete()
        ]
