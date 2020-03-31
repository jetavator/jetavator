import tempfile
import os
import datetime

from jetavator_cli import Client

from behave import given, when, then
from behave_pandas import table_to_dataframe


@when(u"test data is loaded into the source table")
def step_impl(context):

    context.dataframe = table_to_dataframe(
        context.table,
        data_types={
            "example_key": "int",
            "example_int": "int",
            "example_optional_varchar": "str"
        }
    )
    context.jetavator.test_data_loader(
        dataframe=context.dataframe
    ).generate_sql(
        table_name="example",
        base_columns={
            k: v
            for k, v in context.dataframe.items()
        }
    )


@then(
    u"the source table will have an additional datetime "
    "column, jetavator_load_dt"
)
def step_impl(context):
    assert context.jetavator.sql_query_single_value(
        """
        SELECT name
        FROM sys.columns
        WHERE name = 'jetavator_load_dt'
        AND object_id = OBJECT_ID('source.example')
        """
    ) == "jetavator_load_dt", "Column jetavator_load_dt does not exist"


@then(
    u"the source table will have an additional bit "
    "column, jetavator_deleted_ind"
)
def step_impl(context):
    assert context.jetavator.sql_query_single_value(
        """
        SELECT name
        FROM sys.columns
        WHERE name = 'jetavator_deleted_ind'
        AND object_id = OBJECT_ID('source.example')
        """
    ) == "jetavator_deleted_ind", "Column jetavator_deleted_ind does not exist"


@then(
    u"the column jetavator_load_dt is populated with "
    "the current UTC timestamp for all rows"
)
def step_impl(context):
    dataframe = context.jetavator.assert_that.sql_exists(
        """
        SELECT DISTINCT jetavator_load_dt
        FROM source.example
        """
    )
    result = dataframe.iloc[0, 0]
    expected_value = datetime.datetime.now() - datetime.timedelta(minutes=15)
    assert result > expected_value, (
        "Expected " + str(expected_value) +
        ", got " + str(result)
        )


@then(
    u"the column jetavator_deleted_ind is populated with "
    "zeroes for all new rows"
)
def step_impl(context):
    dataframe = context.jetavator.assert_that.sql_exists(
        """
        SELECT DISTINCT jetavator_deleted_ind
        FROM source.example
        """
    )
    result = dataframe.iloc[0, 0]
    expected_value = 0
    assert result == expected_value, (
        "Expected " + str(expected_value) +
        ", got " + str(result)
        )
