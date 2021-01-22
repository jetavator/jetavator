import subprocess
import os

from behave import given, when

from behave_pandas import table_to_dataframe


@given(u"a Behave feature file")
def step_impl(context):
    os.makedirs(os.path.join(context.tempfolder, "features"))
    feature_file_path = os.path.join(
        context.tempfolder, "features", "test.feature"
    )
    feature_file = open(feature_file_path, "w")
    feature_file.write(context.text)
    feature_file.close()


@given(u"a Behave feature implementation")
def step_impl(context):
    os.makedirs(os.path.join(context.tempfolder, "features", "steps"))
    implementation_file_path = os.path.join(
        context.tempfolder, "features", "steps", "test.py"
    )
    implementation_file = open(implementation_file_path, "w")
    implementation_file.write(context.text)
    implementation_file.close()


@given(u"a CSV file {csv_filename} saved in a temporary folder")
@when(u"the following CSV file {csv_filename} is saved in the temporary folder")
def step_impl(context, csv_filename):
    context.csv_file_path = os.path.join(
        context.tempfolder,
        csv_filename
    )
    if context.text:
        csv_file = open(context.csv_file_path, "w")
        csv_file.write(context.text)
        csv_file.close()
    elif context.table:
        table_to_dataframe(
            context.table,
            data_types={
                heading: "str"
                for heading in context.table.headings
            }
        ).to_csv(context.csv_file_path, index=False)
    else:
        raise Exception("No table or CSV text found in this step")
