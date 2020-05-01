from behave.fixture import use_fixture_by_tag, fixture_call_params

from fixtures import fixture_registry, fixture_hook_registry, run_fixture_hooks

from jetavator_cli.testing.behave import BehaveConfig


def before_all(context):
    context.jetavator_config = BehaveConfig(context)


def before_tag(context, tag):
    if tag.startswith("fixture."):
        return use_fixture_by_tag(tag, context, fixture_registry)


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
