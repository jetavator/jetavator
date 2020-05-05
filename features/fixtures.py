import shutil
import tempfile
import yaml
import os

from behave import fixture

from client import from_behave_context

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
    context.jetavator_config.model_path = context.tempfolder
    context.jetavator_config.schema_name = jetavator_schema_name(context)
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
        try:
            context.yaml_file_path = os.path.join(
                context.tempfolder,
                f'{context.yaml["type"]}_{name or context.yaml["name"]}.yaml'
            )
        except (KeyError, TypeError):
            _, context.yaml_file_path = tempfile.mkstemp(
                suffix=".yaml",
                dir=context.tempfolder
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


@registered_fixture("fixture.jetavator")
def fixture_jetavator(context):
    context.jetavator = from_behave_context(
        context,
        config=context.jetavator_config
    )
    # context.jetavator.connection.test(master=True)
    yield context.jetavator
    if not (
        bool(context.config.userdata.get("keepdb"))
        or "setup" in context.tags
    ):
        context.jetavator.drop_schemas()
