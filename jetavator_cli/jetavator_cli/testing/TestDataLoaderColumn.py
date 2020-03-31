import pandas

from jetavator_cli.Client import TYPE_DEFAULTS
from .model_functions import (
    column,
    model_function_registry
)

from ..utils import sql_basic_type


class TestDataLoaderColumn(object):

    def __init__(self, sql_generator_table, column_name):
        self._test_data_loader_table = sql_generator_table
        self._column_name = column_name

    @property
    def _model(self):
        return self._test_data_loader_table._model

    @property
    def _dataframe(self):
        return self._test_data_loader_table._dataframe

    @property
    def _mappings(self):
        return self._test_data_loader_table._mappings

    @property
    def _column_definition(self):
        return self._test_data_loader_table._column_definitions[
            self._column_name
        ]

    def _convert_to_series_if_needed(self, value_or_series, desired_length):
        if type(value_or_series) is pandas.Series:
            return value_or_series
        else:
            return pandas.Series([value_or_series] * desired_length)

    def _type_default(self):
        if self._column_definition.get("nullable"):
            return None
        else:
            type_name = sql_basic_type(self._column_definition.get("type"))
            try:
                return TYPE_DEFAULTS[type_name]
            except KeyError:
                raise Exception(
                    "[" + str(self._column_definition.get("type")) + "]" +
                    " is not recognised as a valid data type"
                )

    def _get_mapped_column_or_default(self, process_defaults=True):
        if self._column_name in self._mappings:
            return self._mappings[self._column_name]
        elif process_defaults and "default" in self._column_definition:
            return self._column_definition["default"]
        elif process_defaults:
            return self._type_default()

    def _resolve_in_function_registry(self, function_dict):
        if "column" in function_dict:
            return column(function_dict["column"])
        elif function_dict.get("function") in model_function_registry:
            return model_function_registry[function_dict["function"]](
                *function_dict.get("parameters", {})
            )
        else:
            raise Exception(
                "Malformed column default definition:\n" +
                str(function_dict)
            )

    def _resolve_if_dict(self, value_or_function):
        if type(value_or_function) is dict:
            return self._resolve_in_function_registry(value_or_function)(
                self._test_data_loader_table._generate_dataframe_no_defaults()
            )
        elif callable(value_or_function):
            return value_or_function()
        else:
            return value_or_function

    def generate_series(self, process_defaults=True):
        return self._convert_to_series_if_needed(
            value_or_series=self._resolve_if_dict(
                self._get_mapped_column_or_default(process_defaults),
                ),
            desired_length=len(self._dataframe)
            )
