from .SatellitePipelineModel import SatellitePipelineModel

from sqlalchemy import select, text, Column, alias, literal_column


class SatelliteSourcePipelineModel(
    SatellitePipelineModel, register_as="satellite_source_pipeline"
):

    def pipeline_query(self, load_type="delta"):

        if load_type == "delta":
                source_table = self.definition.source.sql_model.table
        else:
            source_table = self.definition.source.sql_model.history_table

        return self.generate_load_statement(
            sp_name=f"{load_type}_load_sat_{self.definition.satellite.name}",
            record_source=f"source.{self.definition.satellite.name}",
            source_query=source_table,
            load_dt_column=source_table.c.jetavator_load_dt.label(
                "sat_load_dt"),
            deleted_ind_column=source_table.c.jetavator_deleted_ind.label(
                "sat_deleted_ind")
        )
