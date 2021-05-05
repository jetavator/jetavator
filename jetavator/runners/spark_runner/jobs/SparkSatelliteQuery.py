from pyspark.sql import DataFrame
from typing import Dict, Any, Set, Iterator
from collections.abc import Mapping

import jinja2

from lazy_property import LazyProperty

from sqlalchemy import Column, select, cast, alias, literal_column, text, func
from sqlalchemy.sql.expression import Select
from sqlalchemy.types import Boolean

from jetavator.runners.jobs import SatelliteQuery
from jetavator.sql_model.functions import hash_keygen, hash_record
from jetavator.spark import SparkStorageService
from .. import SparkSQLView


class StorageViewConnector(object):

    loaded_view_keys: Set[str] = None
    storage_service: SparkStorageService = None

    def __init__(self, storage_service: SparkStorageService):
        self.loaded_view_keys = set()
        self.storage_service = storage_service

    def add(self, key: str) -> None:
        if key not in self.loaded_view_keys:
            self.loaded_view_keys.add(key)

    def connect_storage_views(self):
        for view_name in self.loaded_view_keys:
            self.storage_service.connect_storage_view(view_name)

    def disconnect_storage_views(self):
        for view_name in self.loaded_view_keys:
            self.storage_service.disconnect_storage_view(view_name)


class StorageTable(object):

    def __init__(self, table_name: str):
        self.table_name = table_name


class StorageViewMapping(Mapping):

    connector: StorageViewConnector = None
    view_names: Dict[str, str] = None

    def __init__(self, connector: StorageViewConnector, view_names: Dict[str, str]):
        self.connector = connector
        self.view_names = view_names

    def __getitem__(self, k: str) -> str:
        if k not in self.view_names:
            raise ValueError(k)
        value = self.view_names[k]
        if isinstance(value, StorageTable):
            self.connector.add(value.table_name)
            return value.table_name
        else:
            return value

    def __getattr__(self, item):
        return self.get(item)

    def __len__(self) -> int:
        return len(self.view_names)

    def __iter__(self) -> Iterator[str]:
        return iter(self.get(k) for k in self.view_names)


class SparkSatelliteQuery(SparkSQLView, SatelliteQuery, register_as='satellite_query'):

    sql_template = '{{job.sql}}'
    checkpoint = True
    global_view = False

    connector: StorageViewConnector = None
    user_query_sql: str = None

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.connector = StorageViewConnector(self.owner.compute_service.vault_storage_service)
        if self.satellite.pipeline.type == "sql":
            self.user_query_sql = jinja2.Template(self.satellite.pipeline.sql).render(self.table_aliases)
        assert self.sql is not None

    def execute(self) -> DataFrame:
        self.connector.connect_storage_views()
        result = super().execute()
        self.connector.disconnect_storage_views()
        return result

    @LazyProperty
    def table_aliases(self) -> Dict[str, Any]:
        return {
            'source': StorageViewMapping(self.connector, {
                source.name: f'source_{source.name}'
                for source in self.satellite.project.sources.values()
            }),
            'hub': {
                hub.name: StorageViewMapping(self.connector, {
                    'current': StorageTable(f'vault_{hub.full_name}'),
                    'updates': (
                        'vault_updates'
                        f'_{hub.full_name}'
                        f'_{self.satellite.full_name}'
                    ),
                })
                for hub in self.satellite.project.hubs.values()
            },
            'link': {
                link.name: StorageViewMapping(self.connector, {
                    'current': StorageTable(f'vault_{link.full_name}'),
                    'updates': (
                        'vault_updates'
                        f'_{link.full_name}'
                        f'_{self.satellite.full_name}'
                    ),
                })
                for link in self.satellite.project.links.values()
            },
            'satellite': {
                satellite.name:  StorageViewMapping(self.connector, {
                    'current': StorageTable(f'vault_now_{satellite.name}'),
                    'history': StorageTable(f'vault_history_{satellite.name}'),
                    'updates': f'vault_updates_{satellite.full_name}'
                })
                for satellite in self.satellite.project.satellites.values()
            }
        }

    @property
    def sql(self) -> str:
        return self.owner.compute_service.compile_sqlalchemy(
            self.pipeline_query())

    def pipeline_query(self) -> Select:
        if self.satellite.pipeline.type == "source":
            return self.source_pipeline_query()
        else:
            return self.sql_pipeline_query()

    def source_pipeline_query(self) -> Select:

        source_table = self.satellite.pipeline.source.table

        return self._build_pipeline_query(
            source_query=source_table,
            load_dt_column=source_table.c.jetavator_load_dt.label(
                "sat_load_dt"),
            deleted_ind_column=source_table.c.jetavator_deleted_ind.label(
                "sat_deleted_ind")
        )

    def sql_pipeline_query(self) -> Select:

        sql_query_columns = [
            Column(key_column)
            for key_column in self.satellite.pipeline.key_columns.values()
        ]
        sql_query_columns += [
            Column(column_name)
            for column_name in self.satellite.columns.keys()
        ]

        if self.satellite.pipeline.load_dt:
            sql_query_columns.append(Column(self.satellite.pipeline.load_dt))

        if self.satellite.pipeline.deleted_ind:
            sql_query_columns.append(Column(self.satellite.pipeline.deleted_ind))

        sql_query = alias(
            text(self.user_query_sql).columns(*sql_query_columns),
            name="sql_query"
        )

        if self.satellite.pipeline.load_dt:
            load_dt_column = sql_query.c[self.satellite.pipeline.load_dt]
        else:
            load_dt_column = func.current_timestamp()

        if self.satellite.pipeline.deleted_ind:
            deleted_ind_column = sql_query.c[self.satellite.pipeline.deleted_ind]
        else:
            deleted_ind_column = literal_column("FALSE")

        return self._build_pipeline_query(
            source_query=sql_query,
            load_dt_column=load_dt_column.label("sat_load_dt"),
            deleted_ind_column=deleted_ind_column.label("sat_deleted_ind")
        )

    def _build_pipeline_query(
            self,
            source_query: Select,
            load_dt_column: Column,
            deleted_ind_column: Column
    ) -> Select:

        source_inner_query = select([
            *[
                source_query.c[key_column].label(f"hub_{hub_name}_key")
                for hub_name, key_column in self.satellite.pipeline.key_columns.items()
            ],
            *[
                cast(source_query.c[column.name], column.type).label(column.name)
                for column in self.satellite.satellite_columns
            ],
            load_dt_column,
            cast(deleted_ind_column, Boolean()).label(deleted_ind_column.name)
        ]).alias("source_inner_query")

        new_satellite_rows = alias(
            select([
                self.satellite.parent.generate_key(
                    source_inner_query
                ).label(self.satellite.parent.key_name),
                *[
                    source_inner_query.c[column.name]
                    for column in self.satellite.parent.link_key_columns
                ],
                func.coalesce(
                    source_inner_query.c.sat_load_dt,
                    func.current_timestamp()
                ).label("sat_load_dt"),
                func.coalesce(
                    source_inner_query.c.sat_deleted_ind,
                    literal_column("FALSE")
                ).label("sat_deleted_ind"),
                literal_column(f"'{self.satellite.name}'").label("sat_record_source"),
                hash_record(
                    source_inner_query,
                    "sat_deleted_ind",
                    self.satellite.columns
                ).label("sat_record_hash"),
                *[
                    source_inner_query.c[column_name]
                    for column_name in self.satellite.columns
                ]
            ]),
            "new_satellite_rows"
        )

        def generate_table_keys(satellite_owner, source_table):
            if satellite_owner.option("hash_key"):
                hash_key = [hash_keygen(
                    source_table.c[satellite_owner.alias_key_name(satellite_owner.name)]
                ).label(satellite_owner.alias_hash_key_name(satellite_owner.name))]
            else:
                hash_key = []
            return hash_key + [
                source_table.c[satellite_owner.alias_key_name(satellite_owner.name)]
            ]

        select_query = select([
            *generate_table_keys(
                self.satellite.parent,
                new_satellite_rows),
            *[
                new_satellite_rows.c[column.name]
                for column in self.satellite.parent.link_key_columns
            ],
            new_satellite_rows.c.sat_load_dt,
            new_satellite_rows.c.sat_deleted_ind,
            new_satellite_rows.c.sat_record_source,
            new_satellite_rows.c.sat_record_hash,
            *[
                new_satellite_rows.c[column_name]
                for column_name in self.satellite.columns
            ]
        ])

        return select_query
