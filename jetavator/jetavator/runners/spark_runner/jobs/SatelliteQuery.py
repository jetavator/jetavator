from typing import List

from jetavator.schema_registry import Satellite
from jetavator.sql_model.functions import hash_record

from sqlalchemy import Column, select, cast, alias, literal_column, text, func
from sqlalchemy.types import *
from sqlalchemy.sql.expression import Select

from .. import SparkSQLView, SparkRunner, SparkJob


class SatelliteQuery(SparkSQLView, register_as='satellite_query'):
    """
    Computes a DataFrame containing the result of the user query defined in
    `Satellite.pipeline` for a particular `Satellite`.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite:       The `Satellite` object containing the query definition.
    """

    sql_template = '{{job.sql}}'
    checkpoint = True
    global_view = False

    def __init__(
            self,
            runner: SparkRunner,
            satellite: Satellite
    ) -> None:
        super().__init__(
            runner,
            satellite
        )
        self.satellite = satellite

    @property
    def name(self) -> str:
        return f'vault_updates_{self.satellite.full_name}'

    @property
    def sql(self) -> str:
        return self.runner.compute_service.compile_delta_lake(
            self.pipeline_query())

    @property
    def dependencies(self) -> List[SparkJob]:
        return [
            *[
                self.runner.get_job('input_keys', self.satellite, satellite_owner)
                for satellite_owner in self.satellite.input_keys
            ],
            *[
                self.runner.get_job('serialise_satellite', dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'satellite'
                   and dep.view in ['current', 'history']
            ],
            *[
                self.runner.get_job('create_source', dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'source'
            ]
        ]

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
            text(self.satellite.pipeline.sql).columns(*sql_query_columns),
            name="sql_query"
        )

        if self.satellite.pipeline.load_dt:
            load_dt_column = sql_query.c[self.satellite.pipeline.load_dt]
        else:
            load_dt_column = func.current_timestamp()

        if self.satellite.pipeline.deleted_ind:
            deleted_ind_column = sql_query.c[self.satellite.pipeline.deleted_ind]
        else:
            deleted_ind_column = literal_column("0")

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
                cast(source_query.c[column.name], column.type)
                    .label(column.name)
                for column in self.satellite.satellite_columns
            ],
            load_dt_column,
            deleted_ind_column
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
                    literal_column("0")
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

        select_query = select([
            *self.satellite.parent.generate_table_keys(
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
