from .TestDataLoaderTable import TestDataLoaderTable


class TestDataLoader(object):

    def __init__(self, jetavator, dataframe, model):
        self.jetavator = jetavator
        self._dataframe = dataframe
        self._model = model

    @property
    def connection(self):
        return self.jetavator.compute_service

    def generate_sql(
        self, table_name, base_columns, meta_columns={}
    ):
        self.jetavator.load_dataframe(
            TestDataLoaderTable(
                sql_generator=self,
                table_name=table_name,
                # The distinction between definition of base_columns and
                # meta_columns is purely for code comprehension -
                # for now, they are processed by the same engine
                mappings={**base_columns, **meta_columns}
            ).generate_dataframe(),
            table_name
        )
