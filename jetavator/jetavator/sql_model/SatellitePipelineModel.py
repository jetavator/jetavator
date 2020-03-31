from .BaseModel import BaseModel

from sqlalchemy import select, text, Column, alias, literal_column, cast


class SatellitePipelineModel(BaseModel, register_as="satellite_pipeline"):

    @property
    def satellite(self):
        return self.definition.satellite

    @property
    def project(self):
        return self.definition.project

    @property
    def performance_hints(self):
        return self.definition.performance_hints

    def performance_log_start(self, type, name, stage):
        return text(
            f"""
            DECLARE @start_{type}_{name}_{stage}
                    AS DATETIME = CURRENT_TIMESTAMP
            """
        )

    def performance_log_end(self, type, name, stage):
        return text(
            f"""
            INSERT INTO [jetavator].[performance_log] (
                        [type],
                        [name],
                        [stage],
                        [start_timestamp],
                        [end_timestamp],
                        [rows]
                        )
                 VALUES (
                        '{type}',
                        '{name}',
                        '{stage}',
                        @start_{type}_{name}_{stage},
                        CURRENT_TIMESTAMP,
                        @@ROWCOUNT
                 )
            """
        )

    def insert_to_hub(self, hub, hub_alias=None, column_name=None):
        column_name = column_name or (
            "hub_" + (hub_alias or hub.name) + "_key"
        )
        return ([
            # hub.sql_model.inserts_table.delete(),
            # hub.sql_model.inserts_pit_table.delete(),
            hub.sql_model.insert_to_inserts_table(self.satellite, column_name),
            # hub.sql_model.insert_to_table,
            # hub.sql_model.insert_to_inserts_pit_table,
            # hub.sql_model.insert_to_updates_pit_table,
            # hub.sql_model.insert_to_pit_table,
        ])

    def insert_to_link(self, link):
        if self.performance_hints.no_update_hubs:
            update_hubs = []
        else:
            update_hubs = [
                # self.performance_log_start(
                #     "satellite", self.satellite.name, "update_hubs"),
                *[
                    statement
                    for hub_name, hub in link.link_hubs.items()
                    for statement in self.insert_to_hub(hub, hub_name)
                ],
                # self.performance_log_end(
                #     "satellite", self.satellite.name, "update_hubs")
            ]
        return ([
            *update_hubs,
            # link.sql_model.inserts_table.delete(),
            # link.sql_model.inserts_pit_table.delete(),
            link.sql_model.insert_to_inserts_table(self.satellite),
            # link.sql_model.insert_to_table,
            # link.sql_model.insert_to_inserts_pit_table,
            # link.sql_model.insert_to_updates_pit_table,
            # link.sql_model.insert_to_pit_table
        ])

    def compress_versions_in_query(self):
        if self.performance_hints.no_compress_versions_in_query:
            return []
        else:
            return [
                # self.performance_log_start(
                #     "satellite",
                #     self.satellite.name,
                #     "compress_versions_in_query"),
                self.satellite.sql_model.compress_versions_in_query,
                # self.performance_log_end(
                #     "satellite",
                #     self.satellite.name,
                #     "compress_versions_in_query")
            ]

    def compress_versions_in_history(self):
        if self.performance_hints.no_compress_versions_in_history:
            return []
        else:
            return [
                # self.performance_log_start(
                #     "satellite",
                #     self.satellite.name,
                #     "compress_versions_in_history"),
                self.satellite.sql_model.compress_versions_in_query,
                # self.performance_log_end(
                #     "satellite",
                #     self.satellite.name,
                #     "compress_versions_in_history")
            ]

    def compress_versions(self):
        if self.performance_hints.no_compress_versions:
            return []
        else:
            return [
                *self.compress_versions_in_query(satellite),
                *self.compress_versions_in_history(satellite)
            ]

    def insert_to_parent(self):
        if self.satellite.parent.type == "hub" and not self.performance_hints.no_update_hubs:
            return [
                # self.performance_log_start(
                #     "satellite", self.satellite.name, "update_hubs"),
                *self.insert_to_hub(self.satellite.parent),
                # self.performance_log_end(
                #     "satellite", self.satellite.name, "update_hubs")
            ]
        elif self.satellite.parent.type == "link" and not self.performance_hints.no_update_links:
            return [
                # self.performance_log_start(
                #     "satellite", self.satellite.name, "update_links"),
                *self.insert_to_link(self.satellite.parent),
                # self.performance_log_end(
                #     "satellite", self.satellite.name, "update_links")
            ]
        else:
            return []

    def update_referenced_hubs(self):
        if (
            self.satellite.hub_reference_columns
            and not self.performance_hints.no_update_referenced_hubs
        ):
            return [
                # self.performance_log_start(
                #     "satellite",
                #     self.satellite.name, "update_referenced_hubs"),
                *[
                    statement
                    for column_name, column
                    in self.satellite.hub_reference_columns.items()
                    for statement in self.insert_to_hub(
                        self.project.hubs[column.hub_reference],
                        column_name=column_name)
                ],
                # self.performance_log_end(
                #     "satellite", self.satellite.name, "update_referenced_hubs")
            ]
        else:
            return []

    def load_satellite(self):
        return [
            # self.performance_log_start(
            #     "satellite", self.satellite.name, "load_satellite"),
            self.satellite.sql_model.load_satellite,
            # self.performance_log_end(
            #     "satellite", self.satellite.name, "load_satellite")
        ]

    def update_pit_table(self):
        return [
            # self.performance_log_start(
            #     "satellite", self.satellite.name, "update_pit_table"),
            self.satellite.sql_model.insert_to_inserts_pit_table,
            self.satellite.parent.sql_model.insert_to_updates_pit_table,
            self.satellite.parent.sql_model.insert_to_pit_table,
            # self.performance_log_end(
            #     "satellite", self.satellite.name, "update_pit_table")
        ]

    def generate_load_statement(
        self,
        sp_name,
        record_source,
        source_query,
        load_dt_column,
        deleted_ind_column
    ):

        caller = select([
            *[
                source_query.c[key_column].label(f"hub_{hub_name}_key")
                for hub_name, key_column in self.definition.key_columns.items()
            ],
            *[
                cast(source_query.c[column.name], column.type)
                .label(column.name)
                for column in self.satellite.sql_model.satellite_columns
            ],
            load_dt_column,
            deleted_ind_column
        ]).alias("source_query")

        return self.satellite.sql_model.run_pipeline_query(caller)

    def pipeline_query(self, load_type="delta"):
        raise NotImplementedError
