import datetime
import yaml
import os
import pprint

from behave import given, when, then


def schema_registry_value(context, column, entity_type, entity_name):
    return context.jetavator.sql_query_single_value(
        f"""
        SELECT {column}
        FROM [jetavator].[objects]
        WHERE type = '{entity_type}'
            AND name = '{entity_name}'
        """
    )


@then(u"the schema registry contains the following objects")
def step_impl(context):
    for row in context.table:

        type = row["type"]
        name = row["name"]

        yaml_file_path = os.path.join(
            context.tempfolder,
            f"{type}_{name}.yaml"
        )

        with open(yaml_file_path, 'r') as stream:
            yaml_file_contents = yaml.load(stream)

        registry_contents = context.jetavator.schema_registry.deployed[
            type, name
        ].definition

        pp = pprint.PrettyPrinter()
        assert yaml_file_contents == registry_contents, (
            f"""
            YAML definitions do not match for type {type} and name {name}.

            File contents:
            {pp.pformat(yaml_file_contents)}

            Schema registry contents:
            {pp.pformat(registry_contents)}
            """
        )


@then(
    u"the schema registry value {column} "
    "for the {entity_type} {entity_name} is the default timestamp"
)
def step_impl(context, column, entity_type, entity_name):
    result = schema_registry_value(context, column, entity_type, entity_name)
    expected_value = datetime.datetime.strptime("1900-01-01", "%Y-%m-%d")
    assert result == expected_value, (
        "Expected " + str(expected_value) +
        ", got " + str(result)
        )


@then(
    u"the schema registry value {column} "
    "for the {entity_type} {entity_name} is the current timestamp"
)
def step_impl(context, column, entity_type, entity_name):
    result = schema_registry_value(context, column, entity_type, entity_name)
    expected_value = datetime.datetime.now() - datetime.timedelta(minutes=15)
    assert result > expected_value, (
        "Expected > " + str(expected_value) +
        ", got " + str(result)
        )
