from behave.fixture import use_fixture_by_tag

from fixtures import fixture_registry, run_fixture_hooks

from dotenv import load_dotenv


def before_all(context):
    load_dotenv()


def before_tag(context, tag):
    if tag.startswith("fixture."):
        return use_fixture_by_tag(tag, context, fixture_registry)
    run_fixture_hooks(context, tag, "before_tag")


def after_tag(context, tag):
    run_fixture_hooks(context, tag, "after_tag")


def before_step(context, step):
    run_fixture_hooks(context, step, "before_step")


def after_step(context, step):
    run_fixture_hooks(context, step, "after_step")


def before_scenario(context, scenario):
    run_fixture_hooks(context, scenario, "before_scenario")


def after_scenario(context, scenario):
    run_fixture_hooks(context, scenario, "after_scenario")


def before_feature(context, feature):
    run_fixture_hooks(context, feature, "before_feature")


def after_feature(context, feature):
    run_fixture_hooks(context, feature, "after_feature")
