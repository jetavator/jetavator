from .BaseModel import BaseModel

from .functions import hash_record

from sqlalchemy import Column, Index, PrimaryKeyConstraint
from sqlalchemy import select, case, and_, text, subquery

from sqlalchemy.types import *
# from sqlalchemy.dialects.mssql import BIT
from sqlalchemy.sql.expression import func, literal_column, cast, alias
from sqlalchemy.sql.functions import coalesce, max


class SatelliteModel(BaseModel, register_as="satellite"):

    @property
    def files(self):

        files = []

        if self.definition.action in ["create", "drop"]:

            # tables must be created first
            if self.definition.action == "create":
                files += self.create_tables(self.tables)

            # tables must be dropped last
            if self.definition.action == "drop":
                files += self.drop_tables(self.tables)

        return files

    @property
    def tables(self):
        return [
            self.table,
            # self.updates_table
        ]

    @property
    def history_views(self):
        return [
            self.create_or_drop_view(
                self.history_view,
                self.history_view_query)
        ]

    @property
    def current_views(self):
        return [
            self.create_or_drop_view(
                self.current_view,
                self.current_view_query)
        ]

    @property
    def parent(self):
        return self.definition.parent

    @property
    def date_columns(self):
        return [
            Column("sat_load_dt", DATETIME, nullable=True),
            Column("sat_deleted_ind", CHAR(1), nullable=True, default=0)
        ]

    @property
    def record_source_columns(self):
        return [
            Column("sat_record_source", VARCHAR(256), nullable=True),
            Column("sat_record_hash", CHAR(32), nullable=True)
        ]

    @property
    def expiry_date_column(self):
        return Column("sat_expiry_dt", DATETIME, nullable=True)

    @property
    def latest_version_ind_column(self):
        return Column("sat_latest_version_ind", CHAR(1), nullable=True)

    @property
    def satellite_columns(self):
        return [
            Column(
                column_name,
                eval(column.type.upper().replace("MAX", "None")),
                nullable=True
            )
            for column_name, column in self.definition.columns.items()
        ]

    @property
    def link_key_columns(self):
        if self.parent.type == "link":
            return [
                hub.sql_model.alias_key_column(alias)
                for alias, hub in self.parent.link_hubs.items()
            ]
        else:
            return []

    def index_or_key(self, name):
        if self.parent.option("no_primary_key"):
            return self.parent.sql_model.index(name)
        else:
            return PrimaryKeyConstraint(
                self.parent.sql_model.primary_key_name,
                "sat_load_dt",
                name=f"pk_{name}",
                mssql_clustered=self.parent.option("mssql_clustered")
            )

    @property
    def table(self):
        return self.define_table(
            f"vault_sat_{self.definition.name}",
            *self.parent.sql_model.key_columns,
            *self.link_key_columns,
            *self.date_columns,
            *self.record_source_columns,
            *self.satellite_columns,
            # self.index_or_key(f"sat_{self.definition.name}"),
            # *self.custom_indexes(f"sat_{self.definition.name}"),
            schema=self.schema
        )

    def custom_indexes(self, table_name):
        return [
            Index(
                f"ix_{table_name}_ix_{column_name}",
                column_name,
                unique=False
            )
            for column_name, column in self.definition.columns.items()
            if column.index
        ]

    @property
    def view_columns(self):
        return [
            *self.parent.sql_model.key_columns,
            *self.link_key_columns,
            *self.date_columns,
            self.expiry_date_column,
            *self.record_source_columns,
            self.latest_version_ind_column,
            *self.satellite_columns
        ]

    @property
    def updates_view(self):
        return self.define_table(
            f'vault_updates_{self.definition.name}',
            *self.view_columns,
            schema=self.schema
        )

    @property
    def updates_view_query(self):
        return select([
            *self.columns_in_table(
                self.updates_table, self.parent.sql_model.key_columns),
            *self.columns_in_table(
                self.updates_table, self.link_key_columns),
            *self.columns_in_table(
                self.updates_table, self.date_columns),
            literal_column("NULL").label("sat_expiry_dt"),
            *self.columns_in_table(
                self.updates_table, self.record_source_columns),
            literal_column("1").label("sat_latest_version_ind"),
            *self.columns_in_table(
                self.updates_table, self.satellite_columns),
        ])

    @property
    def parent_join_clauses(self):
        return [
            (
                self.table.c[column.name] ==
                self.parent.sql_model.table.c[column.name]
            )
            for column in self.parent.sql_model.key_columns
        ]

    @property
    def expiry_date_expression(self):
        return coalesce(
            max(self.table.c.sat_load_dt).over(
                partition_by=self.columns_in_table(
                    self.table, self.parent.sql_model.key_columns),
                order_by=self.table.c.sat_load_dt,
                rows=(1, 1)
            ),
            literal_column("CAST('9999-12-31 00:00' AS DATE)")
        )

    @property
    def latest_version_ind_expression(self):
        return case(
            {1: 1},
            value=func.row_number().over(
                partition_by=self.columns_in_table(
                    self.table, self.parent.sql_model.key_columns),
                order_by=self.table.c.sat_load_dt.desc()
            ),
            else_=0
        )

    @property
    def history_view(self):
        return self.define_table(
            f'vault_history_{self.definition.name}',
            *self.view_columns,
            schema=self.schema
        )

    @property
    def history_view_query(self):
        return select([
            *self.columns_in_table(
                self.table, self.parent.sql_model.key_columns),
            *self.columns_in_table(
                self.table, self.link_key_columns),
            *self.columns_in_table(
                self.table, self.date_columns),
            self.expiry_date_expression.label("sat_expiry_dt"),
            *self.columns_in_table(
                self.table, self.record_source_columns),
            self.latest_version_ind_expression.label("sat_latest_version_ind"),
            *self.columns_in_table(
                self.table, self.satellite_columns),
        ]).select_from(
            self.table
        )

    @property
    def current_view(self):
        return self.define_table(
            f'vault_now_{self.definition.name}',
            *self.view_columns,
            schema=self.schema
        )

    @property
    def current_view_query(self):
        return select([
            *self.columns_in_table(
                self.history_view, self.parent.sql_model.key_columns),
            *self.columns_in_table(
                self.history_view, self.link_key_columns),
            *self.columns_in_table(
                self.history_view, self.date_columns),
            self.history_view.c.sat_expiry_dt,
            *self.columns_in_table(
                self.history_view, self.record_source_columns),
            self.history_view.c.sat_latest_version_ind,
            *self.columns_in_table(
                self.history_view, self.satellite_columns),
        ]).where(
            and_(
                self.history_view.c.sat_latest_version_ind == 1,
                self.history_view.c.sat_deleted_ind == 0
            )
        )

    def run_pipeline_query(self, caller):

        new_satellite_rows = alias(
            select([
                self.parent.sql_model.generate_key(
                    caller
                    ).label(self.parent.sql_model.key_name),
                *self.columns_in_table(
                    caller, self.link_key_columns),
                func.coalesce(
                    caller.c.sat_load_dt,
                    func.current_timestamp()
                    ).label("sat_load_dt"),
                func.coalesce(
                    caller.c.sat_deleted_ind,
                    literal_column("0")
                    ).label("sat_deleted_ind"),
                literal_column(f"'{self.definition.name}'").label("sat_record_source"),
                hash_record(
                    caller,
                    "sat_deleted_ind",
                    self.definition.columns
                    ).label("sat_record_hash"),
                *[
                    caller.c[column_name]
                    for column_name in self.definition.columns
                ]
            ]),
            "new_satellite_rows"
        )

        select_query = select([
            *self.parent.sql_model.generate_table_keys(
                new_satellite_rows),
            *self.columns_in_table(
                new_satellite_rows, self.link_key_columns),
            new_satellite_rows.c.sat_load_dt,
            new_satellite_rows.c.sat_deleted_ind,
            new_satellite_rows.c.sat_record_source,
            new_satellite_rows.c.sat_record_hash,
            *[
                new_satellite_rows.c[column_name]
                for column_name in self.definition.columns
            ]
        ])

        return select_query

    @property
    def compress_versions_in_query(self):

        prev_record_hash_query = select([
            *self.columns_in_table(
                self.updates_table,
                self.parent.sql_model.key_columns),
            self.table.c.sat_load_dt,
            self.updates_table.c.sat_record_hash,
            max(self.updates_table.c.sat_record_hash).over(
                partition_by=self.columns_in_table(
                    self.updates_table,
                    self.parent.sql_model.key_columns),
                order_by=self.table.c.sat_load_dt,
                rows=(1, 1)
            ).label("prev_record_hash")
        ]).alias("prev_record_hash_query")

        rows_to_delete_query = prev_record_hash_query.select().where(
            prev_record_hash_query.c.sat_record_hash ==
            prev_record_hash_query.c.prev_record_hash
        ).alias("rows_to_delete_query")

        return self.updates_table.delete().where(
            and_(
                *[
                    (
                        rows_to_delete_query.c[column.name] ==
                        self.updates_table.c[column.name]
                    )
                    for column in self.parent.sql_model.key_columns
                ],
                rows_to_delete_query.c.sat_load_dt
                == self.updates_table.c.sat_load_dt
            )
        )

    @property
    def compress_versions_in_history(self):
        return self.updates_table.delete().where(
            and_(
                *[
                    (
                        self.current_view.c[column.name] ==
                        self.updates_table.c[column.name]
                    )
                    for column in self.parent.sql_model.key_columns
                ],
                self.current_view.c.sat_record_hash
                == self.updates_table.c.sat_record_hash
            )
        )

    @property
    def load_satellite(self):
        return self.table.insert().from_select(
            self.table.c,
            select(self.columns_in_table(self.updates_table, self.table.c))
        )

    @property
    def insert_to_inserts_pit_table(self):
        if self.parent.type == "hub":
            role_specific_columns = self.columns_in_table(
                self.updates_table,
                self.parent.sql_model.role_specific_columns)
        elif self.parent.type == "link":
            role_specific_columns = [
                column
                for hub_alias, hub in self.parent.link_hubs.items()
                for column in hub.sql_model.generate_table_keys(
                    self.updates_table, hub_alias)
            ]
        return self.parent.sql_model.inserts_pit_table.insert().from_select(
            self.parent.sql_model.inserts_pit_table.c,
            select([
                *self.columns_in_table(
                    self.updates_table, self.parent.sql_model.key_columns),
                self.updates_table.c.sat_load_dt,
                literal_column(f"'{self.definition.name}'").label("sat_name"),
                *role_specific_columns
            ])
        )

    def generate_update_flag(self, pit_table_name):
        return (
            text(
                f"""
                DECLARE @updated_{self.definition.name} char(1);
                SET @updated_{self.definition.name} = (
                    CASE WHEN EXISTS (
                        SELECT *
                        FROM [vault].[{pit_table_name}]
                        WHERE [sat_name] = '{self.definition.name}'
                    )
                    THEN 1 ELSE 0 END
                )
                """
            )
        )

    @property
    def vault_rollback(self):
        return [
            self.table.delete().where(
                and_(
                    *[
                        (
                            self.table.c[column.name] ==
                            self.updates_table.c[column.name]
                        )
                        for column in self.parent.sql_model.key_columns
                    ],
                    self.table.c.sat_load_dt
                    == self.updates_table.c.sat_load_dt
                )
            ),
            self.updates_table.delete()
        ]

    @property
    def vault_delete_all(self):
        return [
            self.table.delete()
        ]
