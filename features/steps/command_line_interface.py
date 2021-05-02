import subprocess
import os
import csv

from behave import given, when


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
