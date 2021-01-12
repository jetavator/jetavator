from behave import given, when, then


@when(u"we write a new {type} definition to disk")
def step_impl(context, type):
    assert context.feature.new_object_yaml is not None
    assert context.feature.new_object_yaml["type"] == type
    context.yaml = context.feature.new_object_yaml
    context.raw_yaml = context.feature.new_object_raw_yaml
    context.save_yaml_to_tempfolder(context)


@given(u"a definition for a new {type} that we will deploy later")
def step_impl(context, type):
    context.feature.new_object_yaml = context.yaml
    context.feature.new_object_raw_yaml = context.raw_yaml


@when(
    u"we add a new {type} with "
    u".add(yaml_object, "
    u"load_full_history={load_full_history})"
)
def step_impl(context, type, load_full_history):
    context.jetavator.add(
        context.feature.new_object_yaml,
        load_full_history=bool(load_full_history)
    )

@when(
    u"we reload the model in Client with "
    u".update_model_from_dir()"
)
def step_impl(context):
    context.jetavator.update_model_from_dir()


@when(
    u"the definitions are updated with "
    u".update_database_model"
    u"(load_full_history={load_full_history})"
)
@when(
    u"we update the definitions with "
    u".update_database_model"
    u"(load_full_history={load_full_history})"
)
def step_impl(context, load_full_history):
    context.jetavator.update_database_model(
        load_full_history=bool(load_full_history)
    )
