import pandas
import os
from tabulate import tabulate

ATTRIBUTES = [
    "type",
    "pk",
    "nullable",
    "default",
    "aggregation"
]


def print_value(value_or_function):
    if callable(value_or_function):
        return value_or_function.friendly_name
    elif value_or_function is None:
        return ""
    else:
        return str(value_or_function)


def attribute_series(model_table, attribute):
    return pandas.Series(
        [print_value(x.get(attribute)) for x in model_table.values()]
    )


def name_series(model_table):
    return pandas.Series(
        [x for x in model_table]
    )


def model_to_dataframe(model_table):
    return pandas.DataFrame.from_dict({
        **{
            "name": name_series(model_table)
        },
        **{
            x: attribute_series(model_table, x)
            for x in ATTRIBUTES
        }
    })


def dataframe_to_markdown(dataframe):
    return tabulate(
        dataframe,
        headers="keys",
        tablefmt="pipe",
        showindex="never"
    )


def print_models(model, directory="base_table_docs"):

    if not os.path.exists(directory):
        os.makedirs(directory)

    for name, model_table in model.items():
        f = open(os.path.join(directory, name + ".md"), "w")
        f.write(
            dataframe_to_markdown(
                model_to_dataframe(model_table)
            )
        )
        f.close()
