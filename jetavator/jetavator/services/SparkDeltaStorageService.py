from typing import Iterable, Dict

import datetime
import os
import tempfile
import jinja2
import numpy as np
import sqlalchemy
import pandas
import wysdom
import pyspark

from lazy_property import LazyProperty

from jetavator.config import StorageServiceConfig
from jetavator.sqlalchemy_delta import HiveWithDDLDialect, DeltaDialect

from .Service import Service
from .SparkStorageService import SparkStorageService

DELTA_VERSION = 'delta-core_2.12:0.7.0'


class SparkDeltaStorageConfig(StorageServiceConfig):
    type: str = wysdom.UserProperty(wysdom.SchemaConst('spark_delta'))


class SparkDeltaStorageService(
    SparkStorageService,
    Service[SparkDeltaStorageConfig],
    register_as="spark_delta"
):

    @property
    def spark(self):
        return self.owner.spark

    @classmethod
    def compile_hive(cls, sqlalchemy_executable):
        return cls.compile_sqlalchemy_with_dialect(
            sqlalchemy_executable, HiveWithDDLDialect())

    def load_dataframe(
            self,
            dataframe: pandas.DataFrame,
            source_name: str,
            source_column_names: Iterable[str]
    ) -> None:
        for column in source_column_names:
            if column not in dataframe.columns:
                dataframe[column] = np.nan
        if 'jetavator_load_dt' not in dataframe.columns:
            dataframe['jetavator_load_dt'] = datetime.datetime.now()
        if 'jetavator_deleted_ind' not in dataframe.columns:
            dataframe['jetavator_deleted_ind'] = 0
        columns = list(source_column_names) + [
            'jetavator_load_dt',
            'jetavator_deleted_ind'
        ]
        filename = f'{source_name}.csv'
        with tempfile.TemporaryDirectory() as temp_path:
            temp_csv_file = os.path.join(temp_path, filename)
            (
                dataframe
                .reindex(
                    columns=columns)
                .to_csv(
                    temp_csv_file,
                    index=False)
            )
            self.load_csv(temp_csv_file, source_name)

    def load_csv(self, csv_file, source_name: str):
        raise NotImplementedError

    def csv_file_path(self, source_name: str):
        raise NotImplementedError

    def table_delta_path(self, sqlalchemy_table):
        return (
            '/tmp'
            f'/{self.config.schema}'
            f'/{sqlalchemy_table.name}'
        )

    def sql_query_single_value(self, sql):
        return self.execute(sql).iloc[0, 0]

    def test(self):
        assert self.sql_query_single_value("SELECT 1") == 1
        return True

    @LazyProperty
    def sqlalchemy_dialect(self) -> sqlalchemy.engine.interfaces.Dialect:
        return DeltaDialect()

    def merge_from_spark_view(
            self,
            storage_table_name: str,
            spark_view_name: str,
            key_column_name: str,
            column_names: Iterable[str],
            column_references: Dict[str, str]
    ):
        # TODO: Switch from key_source array to indicator columns
        merge_sql_template = '''
            MERGE 
             INTO {{ target }} AS target
            USING {{ source }} AS source
               ON target.{{ key_column_name }}
                = source.{{ key_column_name }}
             WHEN MATCHED AND source.deleted_ind = 1 THEN DELETE
             {% for column, satellite_name in column_references.items() %}
             {{ "WHEN MATCHED THEN UPDATE SET" if loop.first }}
                 {{ column }} = CASE WHEN update_ind_{{ satellite_name }} = 1
                 THEN source.{{ column }}
                 ELSE target.{{ column }}
                 END
               {{ "," if not loop.last }}
             {% endfor %}
             WHEN NOT MATCHED THEN INSERT *
            '''
        merge_sql = jinja2.Template(merge_sql_template).render(
            target=f"{self.config.schema}.{storage_table_name}",
            source=spark_view_name,
            key_column_name=key_column_name,
            column_references=column_references
        )
        self.execute(merge_sql)

    def qualified_table_name(self, table_name: str) -> str:
        return f"{self.config.schema}.{table_name}"

    def read_table(self, table_name: str) -> pyspark.sql.DataFrame:
        return (
            self
            .spark
            .table(self.qualified_table_name(table_name))
        )

    def write_table(
            self,
            table_name: str,
            df: pyspark.sql.DataFrame,
            mode: str = "append"
    ) -> None:
        self.check_valid_mode(mode)
        (
            df
            .write
            .format("delta")
            .mode(mode)
            .saveAsTable(self.qualified_table_name(table_name))
        )

    def connect_storage_view(self, table_name: str) -> None:
        (
            self
            .spark
            .table(self.qualified_table_name(table_name))
            .createOrReplaceTempView(table_name)
        )

    def disconnect_storage_view(self, table_name: str) -> None:
        self.execute(f"DROP VIEW {table_name}")
