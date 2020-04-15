from jetavator_cli import Client

from behave import given, when, then


@given(u'we create a Client instance')
def step_impl(context):
    context.jetavator = Client(context.jetavator_config)


@when(u'we run the test_sql_connection method')
def step_impl(context):
    context.result = context.jetavator.compute_service.test()


@then(u'the result is True')
def step_impl(context):
    assert context.result is True
