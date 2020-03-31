import os

from behave import given, when, then


@when(
    u'we drop a {type} with .drop("{type}", "{name}")'
)
def step_impl(context, type, name):
    context.jetavator.drop(type, name)


@when(u'we delete the {type} definition {name} from disk')
def step_impl(context, type, name):
    os.remove(
        os.path.join(
            context.tempfolder, f"{type}_{name}.yaml"
        )
    )
