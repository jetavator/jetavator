from behave import given, when, then


@when(u'we add a new source')
def step_impl(context):
    try:
        context.jetavator.add(
            context.yaml
        )
    except Exception as e:
        print(e)
        context.exception = e
