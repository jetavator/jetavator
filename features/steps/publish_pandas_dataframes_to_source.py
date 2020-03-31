from behave import given, when, then
from behave_pandas import table_to_dataframe


@when(
    u"we publish below pandas dataframe as "
    u"source {table_name} using {use_as_pk} as primary key"
)
def step_impl(context, table_name, use_as_pk):
    context.table = getattr(context.scenario, "table", context.table)
    context.my_dataframe = table_to_dataframe(context.table, column_levels=1)
    context.jetavator.add_dataframe_as_source(
        context.my_dataframe,
        table_name,
        use_as_pk
    )
