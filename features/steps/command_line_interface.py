import subprocess
import os
import csv

from behave import given, when


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
        with open(context.csv_file_path, "w") as csv_file:
            csv_file.write(context.text)
    elif context.table:
        with open(context.csv_file_path, "w") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(context.table.headings)
            for row in context.table:
                csv_writer.writerow(row.cells)
    else:
        raise Exception("No table or CSV text found in this step")
