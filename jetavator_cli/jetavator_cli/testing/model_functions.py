from collections import Iterable
import pandas


def rank_count(crew_count_column, rank_to_match):
    def dataframe_function(df):
        return pandas.Series([
            crew_count if rank_to_match == rank else 0
            for rank, crew_count in (
                df[["rank", crew_count_column]].itertuples(index=False)
                )
        ])
    dataframe_function.friendly_name = (
        "rank_count('" +
        str(crew_count_column) + "', " +
        str(rank_to_match) + ")"
    )
    return dataframe_function


def rank_count(crew_count_column, rank_to_match):
    def dataframe_function(df):
        return pandas.Series([
            crew_count if rank_to_match == rank else 0
            for rank, crew_count in (
                df[["rank", crew_count_column]].itertuples(index=False)
                )
        ])
    dataframe_function.friendly_name = (
        "rank_count('" +
        str(crew_count_column) + "', " +
        str(rank_to_match) + ")"
    )
    return dataframe_function


def column(column_name_or_names):

    # we want to see if column_name_or_names is iterable e.g. a list,
    # but we do NOT want to iterate over it if it's a string
    # as it will process one letter at a time
    if (
        isinstance(column_name_or_names, Iterable)
        and not type(column_name_or_names) is str
    ):
        column_names = column_name_or_names
    else:
        column_names = [column_name_or_names]

    def dataframe_function(df):
        for column_name in column_names:
            if column_name in df:
                return df[column_name]

    dataframe_function.friendly_name = (
        "column(" + str(column_names) + ")"
    )
    return dataframe_function


model_function_registry = {
    "rank_count": rank_count
}
