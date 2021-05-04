import shutil
import tempfile
import yaml
import os
import jetavator
import datetime

from freezegun import freeze_time

from behave import fixture

fixture_registry = {}
fixture_hook_registry = {}


def registered_fixture(tag):
    def decorator(function):
        fixture_registry[tag] = function

        @fixture
        def wrapper(*args, **kwargs):
            function(*args, **kwargs)
        return wrapper
    return decorator


def fixture_hook(tag, when):
    def decorator(function):
        fixture_hook_registry[tag, when] = function

        def wrapper(*args, **kwargs):
            function(*args, **kwargs)
        return wrapper
    return decorator


def run_fixture_hooks(context, behave_object, when):
    for tag in context.tags:
        if (tag, when) in fixture_hook_registry:
            fixture_hook_registry[tag, when](context, behave_object)


def use_fixture_by_tag(tag, context, fixture_registry):
    fixture_data = fixture_registry.get(tag, None)
    if fixture_data is None:
        raise LookupError("Unknown fixture-tag: %s" % tag)

    fixture_func = fixture_data
    return use_fixture(fixture_func, context)


@registered_fixture("fixture.tempfolder")
def fixture_tempfolder(context, prefix="jetavator_behave_"):
    context.tempfolder = tempfile.mkdtemp(prefix=prefix)
    # context.jetavator_config.model_path = context.tempfolder
    # context.jetavator_config.schema_name = jetavator_schema_name(context)
    yield context.tempfolder
    if not context.config.userdata.get("keep_tempfolder"):
        shutil.rmtree(
            path=context.tempfolder,
            onerror=lambda function, path, excinfo: print(
                f"""
                An error occurred while removing the temporary folder.
                The following folder should be removed manually:
                {path}

                Error details:
                {excinfo}
                """
            )
        )


@registered_fixture("fixture.yaml")
def fixture_yaml(context):

    def save_yaml_to_tempfolder(context, name=None):
        definitions_path = os.path.join(
            context.tempfolder,
            "definitions"
        )
        if not os.path.exists(definitions_path):
            os.makedirs(definitions_path)
        try:
            context.yaml_file_path = os.path.join(
                definitions_path,
                f'{context.yaml["type"]}_{name or context.yaml["name"]}.yaml'
            )
        except (KeyError, TypeError):
            _, context.yaml_file_path = tempfile.mkstemp(
                suffix=".yaml",
                dir=definitions_path
            )

        yaml_file = open(context.yaml_file_path, "w")
        yaml_file.write(context.raw_yaml)
        yaml_file.close()

    context.save_yaml_to_tempfolder = save_yaml_to_tempfolder
    yield None


@fixture_hook("fixture.yaml", when="before_step")
def fixture_yaml_before_step(context, step):
    if not step.text:
        return

    try:
        context.yaml = yaml.load(step.text)
    except yaml.YAMLError as e:
        context.yaml_parse_error = e
        return

    context.raw_yaml = step.text


def jetavator_schema_name(context):
    try:
        return context.config.userdata["db"]
    except KeyError:
        return os.path.split(context.tempfolder)[1]


@registered_fixture("fixture.remove_database_after_scenario")
def fixture_remove_database(context):
    yield None


@fixture_hook("fixture.remove_database_after_scenario", when="after_scenario")
def fixture_remove_after_scenario(context, step):
    engine = jetavator.App(
        jetavator.AppConfig.from_yaml_file(
            jetavator.AppConfig.config_file()))
    engine.logger.info("fixture.remove_database_after_scenario: running")
    engine.drop_schemas()


@registered_fixture("fixture.freeze_time")
def fixture_freeze_time(context):
    yield None


@fixture_hook("fixture.freeze_time", when="after_scenario")
def fixture_freeze_time_after_scenario(context, scenario):
    context.freezer.stop()
