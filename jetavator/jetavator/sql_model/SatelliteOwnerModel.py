from .BaseModel import BaseModel

from .functions import hash_keygen, hash_record

from sqlalchemy import Column, Index, PrimaryKeyConstraint, MetaData
from sqlalchemy import select, case, and_, or_, text
from sqlalchemy.types import *
from sqlalchemy.dialects.mssql import BIT
from sqlalchemy.sql.expression import func, literal_column, cast
from sqlalchemy.sql.functions import coalesce

from sqlalchemy_views import CreateView, DropView
from pyhive import sqlalchemy_hive


class SatelliteOwnerModel(BaseModel, register_as="satellite_owner"):

    @property
    def files(self):
        return self.vault_files() + self.star_files()

    def vault_files(self):
        if self.definition.action == "create":
            return self.create_tables(self.tables)
        else:
            return []

    def star_files(self, with_index=False):
        if self.definition.exclude_from_star_schema:
            return []
        else:
            return (
                self.create_or_alter_tables(self.star_tables, with_index)
            )

    @property
    def create_views(self):
        return [CreateView(
            self.updates_pit_view,
            self.updates_pit_view_query
        )]

    @property
    def drop_views(self):
        return [DropView(self.updates_pit_view)]

    @property
    def tables(self):
        return [
            self.table
        ]

    @property
    def star_tables(self):
        return [
            self.star_table
        ]

    @property
    def star_prefix(self):
        raise NotImplementedError

    def alias_key_name(self, alias):
        return f"{self.type}_{alias}_key"

    def alias_hash_key_name(self, alias):
        return f"{self.type}_{alias}_hash"

    @property
    def key_name(self):
        return self.alias_key_name(self.definition.name)

    @property
    def hash_key_name(self):
        return self.alias_hash_key_name(self.definition.name)

    def alias_key_column(self, alias):
        return Column(
            self.alias_key_name(alias),
            CHAR(self.definition.key_length),
            nullable=False
        )

    def alias_hash_key_column(self, alias):
        return Column(
            self.alias_hash_key_name(alias),
            CHAR(32),
            nullable=False
        )

    def alias_key_columns(self, alias):
        if self.definition.option("hash_key"):
            return [
                self.alias_hash_key_column(alias),
                self.alias_key_column(alias)
            ]
        else:
            return [
                self.alias_key_column(alias)
            ]

    def alias_primary_key_column(self, alias):
        if self.definition.option("hash_key"):
            return self.alias_hash_key_column(alias)
        else:
            return self.alias_key_column(alias)

    def alias_primary_key_name(self, alias):
        if self.definition.option("hash_key"):
            return self.alias_hash_key_name(alias)
        else:
            return self.alias_key_name(alias)

    @property
    def primary_key_column(self):
        return self.alias_primary_key_column(self.definition.name)

    @property
    def primary_key_name(self):
        return self.alias_primary_key_name(self.definition.name)

    @property
    def key_columns(self):
        return self.alias_key_columns(self.definition.name)

    def index(self, name, alias=None, allow_clustered=True):
        alias = alias or self.definition.name
        mssql_clustered = allow_clustered and self.definition.option(
            "mssql_clustered")
        return Index(
            f"ix_{name}",
            self.alias_primary_key_column(alias),
            unique=False,
            mssql_clustered=mssql_clustered
        )

    def index_or_key(self, name, alias=None, allow_clustered=True):
        alias = alias or self.definition.name
        if self.definition.option("no_primary_key"):
            return self.index(name, alias, allow_clustered)
        else:
            mssql_clustered = allow_clustered and self.definition.option(
                "mssql_clustered")
            return PrimaryKeyConstraint(
                self.alias_primary_key_name(alias),
                name=f"pk_{name}",
                mssql_clustered=mssql_clustered
            )

    def satellite_owner_indexes(self, table_name):
        raise NotImplementedError

    def custom_indexes(self, table_name):
        return [
            index
            for satellite in self.definition.star_satellites.values()
            for index in satellite.sql_model.custom_indexes(table_name)
        ]

    @property
    def record_source_columns(self):
        return [
            Column(f"{self.type}_load_dt", DATETIME(), nullable=True),
            Column(f"{self.type}_record_source", VARCHAR(256), nullable=True),
        ]

    @property
    def role_specific_columns(self):
        return []

    @property
    def table_columns(self):
        return [
            *self.key_columns,
            *self.record_source_columns,
            *self.role_specific_columns
        ]

    @property
    def pit_columns(self):
        return [
            Column("sat_load_dt", DATETIME(), nullable=True),
            Column("sat_name", VARCHAR(124), nullable=True)
        ]

    @property
    def table(self):
        return self.define_table(
            f"vault_{self.type}_{self.definition.name}",
            *self.table_columns,
            self.index_or_key(f"{self.type}_{self.definition.name}"),
            *self.satellite_owner_indexes(
                f"{self.type}_{self.definition.name}"),
            schema=self.schema
        )

    @property
    def star_satellite_columns(self):
        return [
            column
            for satellite in self.definition.star_satellites.values()
            for column in satellite.sql_model.satellite_columns
        ]

    @property
    def star_hash_columns(self):
        return [
            Column("jetavator_load_dt", DATETIME, nullable=True),
            Column("jetavator_deleted_ind", CHAR(1), nullable=True, default=0),
            Column("jetavator_record_hash", CHAR(32), nullable=True),
        ]

    @property
    def star_table_name(self):
        return f"star_{self.star_prefix}_{self.definition.name}"

    @property
    def star_table(self):
        return self.define_table(
            self.star_table_name,
            *self.key_columns,
            *self.role_specific_columns,
            *self.star_satellite_columns,
            self.index_or_key(f"{self.star_prefix}_{self.definition.name}"),
            *self.satellite_owner_indexes(
                f"{self.star_prefix}_{self.definition.name}"),
            *self.custom_indexes(
                f"{self.star_prefix}_{self.definition.name}"),
            schema=self.schema
        )

    @property
    def star_updates_table(self):
        return self.define_table(
            f"updates_star_{self.star_prefix}_{self.definition.name}",
            *self.key_columns,
            *self.role_specific_columns,
            Column("deleted_ind", BIT, nullable=True),
            *[
                Column(f"update_ind_{satellite.name}", BIT, nullable=True)
                for satellite in self.definition.star_satellites.values()
            ],
            *self.star_satellite_columns,
            self.index_or_key(f"{self.star_prefix}_{self.definition.name}"),
            *self.satellite_owner_indexes(
                f"{self.star_prefix}_{self.definition.name}"),
            *self.custom_indexes(
                f"{self.star_prefix}_{self.definition.name}"),
            schema=self.schema
        )

    @property
    def star_history_table(self):
        return self.define_table(
            f"star_history_{self.star_prefix}_{self.definition.name}",
            *self.key_columns,
            *self.role_specific_columns,
            *self.star_satellite_columns,
            *self.star_hash_columns,
            self.index(f"history_{self.star_prefix}_{self.definition.name}"),
            *self.satellite_owner_indexes(
                f"history_{self.star_prefix}_{self.definition.name}"),
            *self.custom_indexes(
                f"history_{self.star_prefix}_{self.definition.name}"),
            schema=self.schema
        )

    @property
    def star_rollback_deleted_or_updated_rows(self):

        subquery_rollback = select([
            *self.star_history_table.c,
            func.max(self.star_history_table.c.jetavator_load_dt).over(
                partition_by=[
                    *self.columns_in_table(
                        self.star_history_table,
                        self.key_columns + self.role_specific_columns),
                ]
            ).label("max_date")
        ]).where(
            and_(*[
                (
                    self.star_history_table.c[column.name] ==
                    self.star_updates_table.c[column.name]
                )
                for column in self.key_columns
            ])
        ).alias("rollback")

        return self.star_table.insert().from_select(
            self.star_table.c,
            select(
                self.columns_in_table(subquery_rollback, self.star_table.c)
            ).where(
                subquery_rollback.c.max_date ==
                subquery_rollback.c.jetavator_load_dt
            )
        )

    @property
    def star_delete_invalidated_rows(self):
        join_clauses = [
            (
                self.star_table.c[column.name] ==
                self.star_updates_table.c[column.name]
            )
            for column in self.key_columns
        ]
        return self.star_table.delete().where(and_(*join_clauses))

    @property
    def star_insert_to_history(self):
        return self.star_history_table.insert().from_select(
            self.star_history_table.c,
            self.star_updates_table.select()
        )

    @property
    def star_insert_to_table(self):
        return self.star_table.insert().from_select(
            self.star_table.c,
            select([
                self.star_updates_table.c[column.name]
                for column in self.star_table.c
            ]).where(
                self.star_updates_table.c.jetavator_deleted_ind
                == literal_column("0")
            )
        )

    @property
    def star_insert_to_updates_table(self):

        satellite_column_expressions = [
            case(
                value=self.updates_pit_view.c[f"updated_{satellite.name}"],
                whens={
                    literal_column("1"):
                    satellite.sql_model.updates_view.c[column_name]
                },
                else_=self.star_table.c[column_name]
            ).label(column_name)
            for satellite in self.definition.star_satellites.values()
            for column_name, column in satellite.columns.items()
        ]

        deleted_ind = literal_column("0")
        for satellite in self.definition.star_satellites.values():
            deleted_ind = deleted_ind.op("|")(
                case(
                    value=self.updates_pit_view.c[f"updated_{satellite.name}"],
                    whens={
                        literal_column("1"):
                        satellite.sql_model.updates_view.c.sat_deleted_ind
                    },
                    else_=literal_column("0")
                )
            )

        table_joins = self.updates_pit_view.outerjoin(
            self.star_table,
            and_(*[
                (
                    self.updates_pit_view.c[column.name] ==
                    self.star_table.c[column.name]
                )
                for column in self.key_columns
            ])
        )
        for satellite in self.definition.star_satellites.values():
            table_joins = table_joins.outerjoin(
                satellite.sql_model.updates_view,
                and_(
                    *[
                        (
                            self.updates_pit_view.c[column.name] ==
                            satellite.sql_model.updates_view.c[column.name]
                        )
                        for column in self.key_columns
                    ],
                    literal_column(f"@updated_{satellite.name}") ==
                    literal_column("1")
                )
            )

        new_star_rows = select([
            *self.columns_in_table(
                self.updates_pit_view, self.key_columns),
            *self.columns_in_table(
                self.updates_pit_view, self.role_specific_columns),
            *satellite_column_expressions,
            literal_column("@new_update_dt").label("jetavator_load_dt"),
            deleted_ind.label("jetavator_deleted_ind")
        ]).select_from(
            table_joins
        ).where(
            or_(*[
                satellite.sql_model.updates_view.c.sat_load_dt >
                literal_column("@last_update_dt")
                for satellite in self.definition.star_satellites.values()
            ])
        ).alias("new_star_rows")

        rows_to_insert = select([
            *self.columns_in_table(
                new_star_rows, self.key_columns),
            *self.columns_in_table(
                new_star_rows, self.role_specific_columns),
            *self.columns_in_table(
                new_star_rows, self.star_satellite_columns),
            coalesce(
                new_star_rows.c.jetavator_load_dt,
                func.current_timestamp()).label("jetavator_load_dt"),
            coalesce(
                new_star_rows.c.jetavator_deleted_ind,
                literal_column("0")).label("jetavator_deleted_ind"),
            hash_record(
                new_star_rows,
                "jetavator_deleted_ind",
                self.definition.hashed_columns
            ).label("jetavator_record_hash")
        ])

        return self.star_updates_table.insert().from_select(
            self.star_updates_table.c,
            rows_to_insert
        )

    @property
    def star_compress_updates_table(self):
        return self.star_updates_table.delete().where(
            and_(
                *[
                    (
                        self.star_history_table.c[column.name] ==
                        self.star_updates_table.c[column.name]
                    )
                    for column in self.key_columns
                ],
                self.star_history_table.c.jetavator_record_hash
                == self.star_updates_table.c.jetavator_record_hash
            )
        )

    @property
    def star_full_insert(self):

        table_joins = self.table
        for satellite in self.definition.star_satellites.values():
            table_joins = table_joins.outerjoin(
                satellite.sql_model.current_view,
                and_(*[
                    self.table.c[column.name] ==
                    satellite.sql_model.current_view.c[column.name]
                    for column in self.key_columns
                ]))

        star_full_query = select([
            *self.columns_in_table(
                self.table, self.key_columns),
            *self.columns_in_table(
                self.table, self.role_specific_columns),
            *[
                column
                for satellite in self.definition.star_satellites.values()
                for column in self.columns_in_table(
                    satellite.sql_model.current_view,
                    satellite.sql_model.satellite_columns
                )
            ]
        ]).select_from(
            table_joins
        ).where(
            or_(*[
                satellite.sql_model.current_view.c[
                    self.key_name
                ].isnot(None)
                for satellite in self.definition.star_satellites.values()
            ])
        )

        return self.star_table.insert().from_select(
            self.star_table.c,
            star_full_query
        )

    def generate_table_keys(self, source_table, alias=None):
        alias = alias or self.definition.name
        if self.definition.option("hash_key"):
            hash_key = [hash_keygen(
                source_table.c[self.alias_key_name(alias)]
                ).label(self.alias_hash_key_name(alias))]
        else:
            hash_key = []
        return hash_key + [
            source_table.c[self.alias_key_name(alias)]
        ]

    @property
    def key_type(self):
        if self.definition.key_type:
            return eval(self.definition.key_type)
        else:
            return CHAR(self.definition.key_length)

    def insert_to_inserts_table(self, satellite, column_name=None):
        raise NotImplementedError

    @property
    def insert_to_table(self):
        return self.table.insert().from_select(
            self.table.c,
            self.inserts_table.select()
        )

    @property
    def insert_to_inserts_pit_table(self):
        return self.inserts_pit_table.insert().from_select(
            self.inserts_pit_table.c,
            select([
                *self.columns_in_table(
                    self.inserts_table, self.key_columns),
                self.inserts_table.c[
                    f"{self.type}_load_dt"
                ].label("sat_load_dt"),
                literal_column("''").label("sat_name"),
                *self.columns_in_table(
                    self.inserts_table, self.role_specific_columns)
            ])
        )

    @property
    def insert_to_updates_pit_table(self):
        return self.updates_pit_table.insert().from_select(
            self.updates_pit_table.c,
            select([
                *self.columns_in_table(
                    self.inserts_table, self.key_columns),
                self.inserts_table.c[
                    f"{self.type}_load_dt"
                ].label("sat_load_dt"),
                literal_column("''").label("sat_name"),
                *self.columns_in_table(
                    self.inserts_table, self.role_specific_columns)
            ])
        )

    @property
    def insert_to_updates_pit_table(self):
        return self.copy_rows(self.inserts_pit_table, self.updates_pit_table)

    @property
    def insert_to_pit_table(self):
        return self.copy_rows(self.inserts_pit_table, self.pit_table)

    def generate_key(self, from_table):
        raise NotImplementedError

    @property
    def vault_rollback(self):
        return [
            self.table.delete().where(
                and_(
                    *[
                        (
                            self.table.c[column.name] ==
                            self.inserts_table.c[column.name]
                        )
                        for column in self.key_columns
                    ]
                )
            ),
            self.inserts_table.delete(),
            self.pit_table.delete().where(
                and_(
                    *[
                        (
                            self.pit_table.c[column.name] ==
                            self.updates_pit_table.c[column.name]
                        )
                        for column in (self.key_columns + self.pit_columns)
                    ]
                )
            ),
            self.updates_pit_table.delete()
        ]

    @property
    def vault_delete_all(self):
        return [
            self.table.delete(),
            # self.inserts_table.delete(),
            # self.pit_table.delete(),
            # self.inserts_pit_table.delete(),
            # self.updates_pit_table.delete()
        ]

    @property
    def star_delete_all(self):
        return [
            self.star_table.delete(),
            # self.star_history_table.delete(),
            # self.star_updates_table.delete()
        ]
