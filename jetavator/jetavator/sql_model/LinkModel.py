from .SatelliteOwnerModel import SatelliteOwnerModel

from sqlalchemy import Column, select, cast, func, literal_column, and_


class LinkModel(SatelliteOwnerModel, register_as="link"):

    @property
    def star_prefix(self):
        return "fact"

    @property
    def hub_key_columns(self):
        return [
            key_column
            for hub_alias, hub in self.definition.link_hubs.items()
            for key_column in hub.sql_model.alias_key_columns(hub_alias)
        ]

    @property
    def role_specific_columns(self):
        return self.hub_key_columns

    def satellite_owner_indexes(self, table_name):
        return [
            hub.sql_model.index(
                f"{table_name}_hx_{hub_alias}",
                hub_alias,
                allow_clustered=False
            )
            for hub_alias, hub in self.definition.link_hubs.items()
        ]

    def insert_to_inserts_table(self, satellite, column_name=None):

        column_name = column_name or self.key_name

        source_subquery = select([
            *self.columns_in_table(
                satellite.sql_model.updates_table, self.key_columns),
            *[
                column
                for hub_name, hub in self.definition.link_hubs.items()
                for column in hub.sql_model.generate_table_keys(
                    satellite.sql_model.updates_table,
                    hub_name
                )
            ]
        ]).distinct().alias("source")

        select_query = select([
            *self.columns_in_table(
                source_subquery, self.key_columns),
            func.current_timestamp(),
            literal_column("@record_source"),
            *self.columns_in_table(
                source_subquery, self.role_specific_columns)
        ]).select_from(
            source_subquery.outerjoin(
                self.table,
                and_(*[
                    self.table.c[column.name] ==
                    source_subquery.c[column.name]
                    for column in self.key_columns
                ]))
        ).where(
            self.table.c[self.key_name].is_(None))

        return select_query

    def generate_key(self, from_table):
        key_components = iter([
            hub.sql_model.prepare_key_for_link(hub_alias, from_table)
            for hub_alias, hub in self.definition.link_hubs.items()
        ])
        composite_key = next(key_components)
        for column in key_components:
            composite_key = composite_key.concat(
                literal_column("'/'")
            ).concat(column)
        return composite_key
