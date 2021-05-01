import os
import random
import shutil
import pandas
import datetime
import logging

from behave import given, when, then, model

from freezegun import freeze_time

from sqlalchemy import MetaData, Table, Column
from sqlalchemy.sql import and_, select

from ast import literal_eval

from jetavator.default_logger import default_logger
from jetavator.cli import main as cli_main
from jetavator import Engine, Config

SCHEMA_METADATA_TABLE = {
    "table": "INFORMATION_SCHEMA.TABLES",
    "view": "INFORMATION_SCHEMA.VIEWS"
}

# use_step_matcher("parse")


class ListenFilter(logging.Filter):
    command_output = ''

    def filter(self, record):
        self.command_output += '\n' + str(record.getMessage())
        return True


def run_cli(
        command,
        print_output=True,
        retry_limit=1,
        break_on_error=True
):

    print(f"Running CLI command: {command}", flush=True)

    log_listener = ListenFilter()

    default_logger.addFilter(log_listener)

    exit_code = 0

    def exit_callback(code):
        exit_code = code

    assert retry_limit > 0, "retry_limit must be greater than zero"

    for attempts in range(0, retry_limit):
        try:
            command_parts = (
                command
                    .replace("\\\r", "")
                    .replace("\\\n", "")
                    .replace("\r", "")
                    .replace("\n", "")
                    .split(' ')
            )
            command, *argv = [
                part.replace('"', '')
                for part in command_parts
                if part != ''
            ]
            assert command == 'jetavator', (
                f'CLI command "{command}" not recognised'
            )
            cli_main(argv, exit_callback)
            break
        except Exception as e:
            if attempts + 1 == retry_limit:
                if break_on_error:
                    raise Exception(
                        f"""
                        Error running bash command: {command}
                        Return code: {exit_code}
                        Output: {str(e)}
                        """
                    )
                else:
                    return log_listener.command_output + str(e), exit_code

    default_logger.removeFilter(log_listener)

    return log_listener.command_output, 0


def jetavator_engine():
    return Engine(Config.from_yaml_file(Config.config_file()))


def engine_datastore(datastore):
    compute = jetavator_engine().compute_service
    return compute.storage_services[compute.config.storage[datastore]]


def engine_metadata(datastore):
    meta = MetaData()
    if hasattr(engine_datastore(datastore), 'sqlalchemy_connection'):
        meta.reflect(
            bind=engine_datastore(datastore).sqlalchemy_connection
        )
    return meta


@given(u"the table {table} on the {datastore} datastore is empty")
def given_table_is_empty(context, table, datastore):
    engine_datastore(datastore).execute(
        f"DELETE FROM {table}"
    )


@when(u"we execute the following SQL on the {datastore} datastore")
def step_impl(context, datastore):
    engine_datastore(datastore).execute(context.text)


@given(u"the system time is {time}")
def step_impl(context, time):

    if "fixture.freeze_time" not in context.tags:
        raise Exception("Must use @fixture.freeze_time")

    context.freezer = freeze_time(time)
    context.freezer.start()


@then(
    u"the {sql_object:w} {table:w} "
    u"on the {datastore} datastore "
    u"contains {row_count} row"
)
@then(
    u"the {sql_object:w} {table:w} "
    u"on the {datastore} datastore "
    u"contains {row_count} rows"
)
def step_impl(context, sql_object, table, row_count, datastore):
    store = engine_datastore(datastore)
    qualified_table_name = f"{store.config.schema}.{table}"
    actual_row_count = store.sql_query_single_value(
        f"""
        SELECT COUNT(*) AS row_count
        FROM {qualified_table_name}
        """
    )
    assert int(actual_row_count) == int(row_count), (
        f"""
        Expected {row_count} rows, got {actual_row_count} rows. The table contents are:
        {store.execute("SELECT * FROM " + qualified_table_name)}
        """
    )


@then(u"the {sql_object:w} {table:w} exists on the {datastore} datastore")
def step_impl(context, sql_object, table, datastore):
    assert engine_datastore(datastore).table_exists(table), (
        f"The {sql_object} {table} does not exist")


@then(u"the {sql_object:w} {table:w} does not exist on the {datastore} datastore")
def step_impl(context, sql_object, table, datastore):
    assert not engine_datastore(datastore).table_exists(table), (
        f"The {sql_object} {table} exists")


@then(u"the following SQL on the {datastore} datastore returns {row_count:d} row")
@then(u"the following SQL on the {datastore} datastore returns {row_count:d} rows")
def step_impl(context, row_count, datastore):
    actual_row_count = engine_datastore(datastore).sql_query_single_value(
        f"""
        SELECT COUNT(*) AS [row_count]
        FROM (
            {context.text}
        ) [query_to_count]
        """
    )
    assert int(actual_row_count) == int(row_count), (
        f"Expected {row_count} rows, got {actual_row_count} rows"
    )


@then(u"the following SQL on the {datastore} datastore returns the value {expected_value}")
def step_impl(context, expected_value, datastore):
    actual_value = engine_datastore(datastore).sql_query_single_value(
        context.text
    )
    assert str(actual_value) == str(expected_value), (
        f"Expected {expected_value}, got {actual_value}"
    )


@given(
    u"a set of test data"
)
def step_impl(context):
    context.feature.test_data = context.table


@when(u'we load a set of test data to table {table:w}')
def step_impl(context, table):
    try:
        load_to_base_table(
            context, table, 'source'
        )
    except Exception as e:
        print(e)
        context.exception = e


@when(
    u"the test data is loaded in source {table:w}"
)
def step_impl(context, table):
    context.table = context.feature.test_data
    load_to_base_table(context, table)


@then(
    u"the column {column} exists "
    u"in the table {table} "
    u"on the {datastore} datastore"
)
def step_impl(context, column, table, datastore):
    assert engine_datastore(datastore).column_exists(
        table, column
    ), (
        f"Could not find expected column {column} in table {table}."
    )


@then(
    u"the column {column} does not exist "
    u"in the table {table} "
    u"on the {datastore} datastore"
)
def step_impl(context, column, table, datastore):
    assert not engine_datastore(datastore).column_exists(
        table, column
    ), (
        f"Unexpectedly found column {column} in table {table}."
    )


@then(
    u"the index {index} exists "
    u"on the column {table}.{column} "
    u"on the {datastore} datastore"
)
def step_impl(context, index, table, column, datastore):
    indexes = engine_metadata(datastore).tables.get(table).indexes
    assert any(
        test_index.name == index and test_column.name == column
        for test_index in indexes
        for test_column in test_index.columns
    ), f"Could not find the expected index in {indexes}."


@then(
    u"the table {table_name:w} "
    u"on the {datastore} datastore "
    u"contains these columns with this data"
)
def step_impl(context, table_name, datastore):
    connection = engine_datastore(datastore)

    meta = MetaData()
    table = Table(
        table_name,
        meta,
        *[
            Column(heading)
            for heading in context.table.headings
        ],
        schema=connection.config.schema
    )

    select_list = [
        table.c[heading]
        for heading in context.table.headings
    ]

    for row in context.table:
        where_clauses = [
            table.c[heading] == (None if row[heading] == "None" else row[heading])
            for heading in row.headings
        ]
        row_results = connection.execute_sql_element(
            select(select_list).where(and_(*where_clauses))
        )
        assert len(row_results) <= 1, (
            f"""
            The following row returned multiple ambiguous results:
            {row}

            The rows returned were:
            {row_results}
            """
        )
        assert len(row_results) >= 1, (
            f"""
            The following row returned no results:
            {row}

            The table contents are:
            {connection.execute_sql_element(select(select_list))}
            """
        )


@given(u"the file {file} is loaded to table {table:w}")
@when(u"the file {file} is loaded to table {table:w}")
def load_file_to_table(context, file, table):
    jetavator_engine().load_csv(
        csv_file=os.path.join(
            context.config.paths[0], "data", file
        ),
        table_name=table
    )


def substitutions(context):
    userdata = dict(context.config.userdata)
    # if 'jetavator_config' in context:
    #     userdata['schema'] = context.jetavator_config.schema
    if 'tempfolder' in context:
        userdata['model_path'] = context.tempfolder
    return userdata


def format_arg_string(context, arg_string):
    substitutions_dict = substitutions(context)
    if 'tempfolder' in context:
        substitutions_dict['tempfolder'] = (
            context.tempfolder.replace("\\", "\\\\"))
    return arg_string.format(**substitutions_dict)


def run_cli_command(context, command, break_on_error=True):
    substitutions_dict = substitutions(context)
    substitutions_dict['config_args'] = ' '.join(
        f'--set {k}="{v}"'
        for k, v in substitutions_dict.items()
        if k not in ('config', 'config_args', 'tempfolder')
    )
    if 'config' in context.config.userdata:
        substitutions_dict['config_args'] += (
            f' --config-file="{context.config.userdata["config"]}"'
        )
    if 'tempfolder' in context:
        substitutions_dict['tempfolder'] = context.tempfolder
    context.cli_stdout, context.cli_error_code = run_cli(
        command.format(**substitutions_dict),
        break_on_error=break_on_error
    )


def get_property(context, expression):
    interim_result = context.jetavator
    for property in expression.split("."):
        interim_result = getattr(interim_result, property)
    context.result = interim_result


def assert_returns_value(context, actual, expected):
    assert str(actual).splitlines() == str(expected).splitlines(), (
        f"""
        Expected: {str(expected)}
        Got: {str(actual)}
        """
    )


def assert_returns_table(context, actual_dataframe, expected_table):
    with pandas.option_context(
            'display.max_rows', None,
            'display.max_columns', None
    ):
        assert isinstance(actual_dataframe, pandas.DataFrame), (
            "actual_dataframe must be type pandas.DataFrame, "
            f"not {type(actual_dataframe)}"
        )
        assert isinstance(expected_table, model.Table), (
            "expected_table must be type behave.model.Table, "
            f"not {type(expected_table)}"
        )
        assert len(expected_table.rows) == actual_dataframe.shape[0], (
            f"""
            Expected {len(expected_table.rows)} row(s),
            got {actual_dataframe.shape[0]}.
            Output dataframe:
            {actual_dataframe}
            """
        )
        for row in expected_table:
            matches = [
                (actual_dataframe[heading] == row[heading])
                for heading in row.headings
                if row[heading] != "*"  # exclude wildcard values
            ]
            assert any(map(all, zip(*matches))), (
                f"""
                Could not find row:
                {row}
                in dataframe:
                {actual_dataframe}
                """
            )


@given(u"we run the CLI command")
@when(u"we run the CLI command")
def step_impl(context):
    run_cli_command(context, context.text)


@given(u'we run the CLI command "{command}"')
@when(u'we run the CLI command "{command}"')
def step_impl(context, command):
    run_cli_command(context, command)


@then(u"the command returns error code {code}")
def step_impl(context, code):
    assert int(context.cli_error_code) == int(code), (
        f"Expected command exit code {code}, got {context.cli_error_code}."
    )


@then(u"the command returns an error with text {text}")
def step_impl(context, text):
    assert int(context.cli_error_code) != 0, (
        f"No cli error was detexted."
    )
    assert text in context.cli_stdout, (
        f"Expected {text}, got {context.cli_stdout}"
    )


@then(u'the CLI command "{command}" returns "{text}"')
def cli_command_returns(context, command, text):
    run_cli_command(context, command)
    for line in text.splitlines():
        assert line in context.cli_stdout, (
            f"Expected {line}, got {context.cli_stdout}"
        )


@then(u'the CLI command "{command}" returns this text')
def step_impl(context, command):
    cli_command_returns(context, command, context.text)


@then(u'the CLI command "{command}" returns this table')
def step_impl(context, command):
    run_cli_command(context, command)
    for row in context.table:
        assert any(
            all(
                (str(cell) == "*" or str(cell) in line)
                for cell in row
            )
            for line in context.cli_stdout.splitlines()
        ), f"Could not find {row} in {context.cli_stdout}"


@given(u'a definition for a {type} named "{name}"')
def yaml_definition_with_name(context, type, name):
    assert context.yaml is not None
    assert context.yaml["type"] == type
    assert "name" in context.yaml

    if "tempfolder" in context:
        context.save_yaml_to_tempfolder(context, name)

    if not hasattr(context.feature, "yaml"):
        context.feature.yaml = {}

    context.feature.yaml[
        context.yaml["type"],
        name
    ] = {
        "parsed": context.yaml,
        "raw": context.raw_yaml
    }


@given(u"a definition for a {type}")
@when(u"we write this new {type} definition to disk")
def yaml_definition(context, type):
    assert context.yaml is not None
    assert context.yaml["name"] is not None
    yaml_definition_with_name(context, type, context.yaml["name"])


@given(u'a folder "{folder_name}" containing the files')
def step_impl(context, folder_name):
    assert "tempfolder" in context
    folder_path = os.path.join(context.tempfolder, folder_name)
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
    os.mkdir(folder_path)
    for line in context.text.splitlines():
        shutil.copyfile(
            os.path.join(context.tempfolder, f"{line}.yaml"),
            os.path.join(folder_path, f"{line}.yaml"),
        )


@when(u"we try to {step_description}")
def step_impl(context, step_description):
    if step_description == "run the CLI command":
        # this behaves differently as it returns an exit code
        # instead of an exception
        run_cli_command(context, context.text, break_on_error=False)
    else:
        if context.text:
            context.scenario.text = context.text
        if context.table:
            context.scenario.table = context.table
        try:
            context.execute_steps(f"When we {step_description}")
        except AssertionError as e:
            context.exception = str(e)


@then(u'an exception is raised')
def step_impl(context):
    assert "exception" in context, (
        "No exception was detected."
    )
    print(context.exception)


@then(u'an exception is raised with text "{text}"')
def step_impl(context, text):
    assert "exception" in context, (
        "No exception was detected."
    )
    assert text in context.exception, (
        f"Expected {text}, got {context.exception}"
    )


@when(u'all the definitions are saved to disk')
def step_impl(context):
    for yaml_file in context.feature.yaml.values():
        file_path = os.path.join(
            context.tempfolder,
            (
                    yaml_file["parsed"]["type"] + "_" +
                    yaml_file["parsed"]["name"] + ".yaml"
            )
        )
        with open(file_path, "w") as f:
            f.write(yaml_file["raw"])


@given(u'we call the method .{expression}')
@when(u'we call the method .{expression}')
def eval_expression(context, expression):
    formatted_expression = format_arg_string(context, expression)
    context.result = eval(f"context.jetavator.{formatted_expression}")


@then(u'the property .{expression} returns this text')
@then(u'the method .{expression} returns this text')
def step_impl(context, expression):
    eval_expression(context, expression)
    assert_returns_value(context, context.result, context.text)


@then(u'the property .{expression} returns this table')
@then(u'the method .{expression} returns this table')
def step_impl(context, expression):
    eval_expression(context, expression)
    assert_returns_table(context, context.result, context.table)


@then(u'the property .{expression} returns {value}')
@then(u'the method .{expression} returns {value}')
def step_impl(context, expression, value):
    eval_expression(context, expression)
    assert_returns_value(context, context.result, literal_eval(value))


@then(u'the property .{expression} is in the last {minutes} minutes')
@then(u'the method .{expression} is in the last {minutes} minutes')
def step_impl(context, expression, minutes):
    eval_expression(context, expression)
    expected_datetime = datetime.datetime.now() - datetime.timedelta(
        minutes=int(minutes))
    assert context.result >= expected_datetime, (
        f"""
        Expected > {str(expected_datetime)}
        Got: {str(context.result)}
        """
    )


@then(u'the property .{expression} is not None')
@then(u'the method .{expression} is not None')
def step_impl(context, expression):
    eval_expression(context, expression)
    assert context.result is not None, (f"{expression} is None")


@then(u'the expression {expression} returns this text')
def step_impl(context, expression):
    assert_returns_value(
        context,
        eval(format_arg_string(context, expression)),
        context.text)


@then(u'the expression {expression} returns this table')
def step_impl(context, expression):
    assert_returns_table(
        context,
        eval(format_arg_string(context, expression)),
        context.table)


@then(u'the expression {expression} returns {value}')
def step_impl(context, expression, value):
    assert_returns_value(
        context,
        eval(format_arg_string(context, expression)),
        literal_eval(value))


@given(u'a random string in the environment variable ${env_var_name}')
def step_impl(context, env_var_name):
    os.environ[env_var_name] = "jetavator_" + "_".join([
        "%04x" % random.randrange(16 ** 4)
        for i in range(0, 3)
    ])


@given(u'a file saved as {filename}')
def step_impl(context, filename):
    with open(os.path.join(context.tempfolder, filename), "w") as f:
        f.write(context.text)


@given(u'a config file saved as {filename}')
def step_impl(context, filename):
    with open(os.path.join(context.config.userdata.get("config"))) as from_file:
        text = from_file.read()
    with open(os.path.join(context.tempfolder, filename), "w") as to_file:
        to_file.write(text)
