from .SatellitePipelineModel import SatellitePipelineModel

from sqlalchemy import select, text, Column, alias, literal_column

from sqlalchemy.sql.expression import func


class SatelliteSQLPipelineModel(
    SatellitePipelineModel, register_as="satellite_sql_pipeline"
):

    def pipeline_query(self, load_type="delta"):

        caller_query_columns = [
            Column(key_column)
            for key_column in self.definition.key_columns.values()
        ]
        caller_query_columns += [
            Column(column_name)
            for column_name in self.definition.satellite.columns.keys()
        ]

        if self.definition.load_dt:
            caller_query_columns.append(Column(self.definition.load_dt))

        if self.definition.deleted_ind:
            caller_query_columns.append(Column(self.definition.deleted_ind))

        caller_query = alias(
            text(self.definition.sql).columns(*caller_query_columns),
            name="caller_query"
        )

        if self.definition.load_dt:
            load_dt_column = caller_query.c[self.definition.load_dt]
        else:
            load_dt_column = func.current_timestamp()

        if self.definition.deleted_ind:
            deleted_ind_column = caller_query.c[self.definition.deleted_ind]
        else:
            deleted_ind_column = literal_column("0")

        return self.generate_load_statement(
            sp_name=f"{load_type}_load_sat_{self.definition.satellite.name}",
            record_source=f"sql.{self.definition.satellite.name}",
            source_query=caller_query,
            load_dt_column=load_dt_column.label("sat_load_dt"),
            deleted_ind_column=deleted_ind_column.label("sat_deleted_ind")
        )
