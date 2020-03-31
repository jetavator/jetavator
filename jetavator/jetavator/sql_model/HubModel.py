from .SatelliteOwnerModel import SatelliteOwnerModel

from sqlalchemy import Column, select, cast, func, literal_column, and_


class HubModel(SatelliteOwnerModel, register_as="hub"):

    @property
    def star_prefix(self):
        return "dim"

    @property
    def static_columns(self):
        return [
            Column(
                column_name,
                eval(column.type),
                nullable=True
            )
            for column_name, column in self.definition.static_columns.items()
        ]

    @property
    def role_specific_columns(self):
        return self.static_columns

    def satellite_owner_indexes(self, table_name):
        return []

    def insert_to_inserts_table(self, satellite, column_name=None):

        column_name = column_name or self.key_name

        hub_data_subquery = select([
            cast(
                satellite.sql_model.updates_table.c[column_name], self.key_type
            ).label(self.key_name)
        ]).where(
            satellite.sql_model.updates_table.c[column_name].isnot(None)
        ).distinct().alias("hub_data")

        source_subquery = select([
            *self.generate_table_keys(hub_data_subquery),
            *self.columns_in_table(
                hub_data_subquery, self.role_specific_columns)
        ]).where(
            hub_data_subquery.c[
                self.key_name
            ].isnot(None)
        ).alias("source")

        select_query = select([
            *self.columns_in_table(
                source_subquery, self.key_columns),
            func.current_timestamp(),
            literal_column("@record_source"),
            *self.columns_in_table(
                source_subquery, self.role_specific_columns)
        ]).select_from(
            source_subquery
        )

        return select_query

    def generate_key(self, from_table):
        return from_table.c[self.key_name]

    def prepare_key_for_link(self, alias, from_table):
        key_column = from_table.c[self.alias_key_name(alias)]
        if self.definition.key_type == "DATETIME":
            return func.convert(VARCHAR, key_column, 126)
        elif self.definition.key_type == "DATE":
            return func.convert(VARCHAR, key_column, 23)
        else:
            return func.upper(func.ltrim(func.rtrim(key_column)))
