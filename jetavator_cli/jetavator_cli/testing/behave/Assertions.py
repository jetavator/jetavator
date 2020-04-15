import pandas


def _trim_whitespace(result, trim_result_whitespace):
    if trim_result_whitespace and isinstance(result, str):
        result = result.strip()
    return result


class Assertions(object):

    def __init__(self, client):
        self.client = client

    def column_exists(self, dataframe, column_name):
        assert column_name in dataframe, (
            "Expected " + column_name +
            " in result columns. Actual columns are " +
            ", ".join(list(dataframe))
            )

    def dataframe_not_empty(self, dataframe):
        assert not dataframe.dropna().empty, (
            "Dataset does not exist in the database"
            )

    def dataframe_empty(self, dataframe):
        assert dataframe.dropna().empty, "Dataset does exist in the database"

    def column_equal_to(
        self,
        dataframe,
        column_name,
        column_value,
        trim_result_whitespace=True
    ):
        self.column_exists(dataframe, column_name)
        for index, row in dataframe.iterrows():
            result = _trim_whitespace(row[column_name], trim_result_whitespace)
            assert str(result) == str(column_value), (
                "Expected " + column_name +
                " = " + str(column_value) +
                ", got " + str(result)
                )

    def sql_exists(self, sql, **kwargs):
        dataframe = self.client.compute_service.execute_to_pandas(
            sql.format(**kwargs)
        )
        assert len(dataframe) > 0, (
            "Expected results for the following query and received none:\n"
            + sql.format(**kwargs)
        )
        return dataframe

    def sql_does_not_exist(self, sql, **kwargs):
        dataframe = self.client.compute_service.execute_to_pandas(
            sql.format(**kwargs)
        )
        assert len(dataframe) == 0, (
            "Expected no results for the following query and received some:\n"
            + sql.format(**kwargs)
        )
        return dataframe

    def sql_equal_to(
        self, sql, expected_value, trim_result_whitespace=True, **kwargs
    ):
        dataframe = self.sql_exists(sql, **kwargs)
        result = _trim_whitespace(
            dataframe.iloc[0, 0],
            trim_result_whitespace
            )
        assert result == expected_value, (
            "Expected " + str(expected_value) +
            ", got " + str(result)
            )
