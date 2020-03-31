import pandas
import datetime
import sqlalchemy

from tabulate import tabulate

from .TestDataLoaderColumn import TestDataLoaderColumn

SOURCE_SCHEMA = "source"

SYSTEM_COLUMNS = {
    "jetavator_load_dt": {
        "type": "datetime",
        "nullable": False,
        "default": (lambda: datetime.datetime.now())
    },
    "jetavator_deleted_ind": {
        "type": "char(1)",
        "nullable": False,
        "default": 0
    }
}


class TestDataLoaderTable(object):

    def __init__(self, sql_generator, table_name, mappings):
        self._test_data_loader = sql_generator
        self._table_name = table_name
        self._mappings = mappings
        self._mapped_columns = {
            column_name: TestDataLoaderColumn(self, column_name)
            for column_name in mappings
        }
        self._columns = {
            column_name: TestDataLoaderColumn(self, column_name)
            for column_name in self._column_definitions
        }

    @property
    def _dataframe(self):
        return self._test_data_loader._dataframe

    @property
    def _primary_key_columns(self):
        return [
            k for k, v
            in self._column_definitions.items()
            if v.get("pk")
        ]

    @property
    def _column_aggregations(self):
        return {
            k: v["aggregation"] for k, v
            in self._column_definitions.items()
            if "aggregation" in v
        }

    @property
    def _column_definitions(self):
        table_columns = self._table_definition["columns"]
        mapped_system_columns = {
            k: v
            for k, v in SYSTEM_COLUMNS.items()
        }
        return {
            **table_columns,
            **mapped_system_columns
        }

    @property
    def _table_definition(self):
        return self._test_data_loader._model[self._table_name].definition

    def _generate_dataframe_no_defaults(self, column_list=None):
        return pandas.DataFrame.from_dict({
            key: column.generate_series(process_defaults=False)
            for key, column in self._mapped_columns.items()
        })

    def _generate_dataframe(self, column_list=None):
        return pandas.DataFrame.from_dict({
            key: column.generate_series()
            for key, column in self._columns.items()
        })

    def _normalise_dataframe(self, dataframe):
        if len(self._column_aggregations) > 0:
            for column_name, aggregation in self._column_aggregations.items():
                dataframe[column_name] = dataframe.groupby(
                    self._primary_key_columns
                )[column_name].transform(aggregation)

        return dataframe.drop_duplicates()

    def _check_dataframe_for_duplicates(self, dataframe):
        if dataframe.duplicated(subset=self._primary_key_columns).any():
            raise Exception(
                "Values are not normalisable across the primary key " +
                "for table " + self._table_name + ".\n" +
                "\tPrimary key: " + str(self._primary_key_columns) + "\n"
                "\tDataframe:\n" + tabulate(dataframe, headers='keys')
            )

    def generate_dataframe(self):
        df = self._generate_dataframe()
        df = self._normalise_dataframe(df)
        if len(self._primary_key_columns) > 0:
            self._check_dataframe_for_duplicates(df)
        return df
