from typing import Iterable, Dict, Optional, List

import jinja2
import sqlalchemy
import wysdom
import pyspark

from lazy_property import LazyProperty

from jetavator.config import StorageServiceConfig
from jetavator.sqlalchemy_delta import DeltaDialect
from jetavator.services import Service

from .SparkStorageService import SparkStorageService, SparkStorageServiceOwner

DRIVER_GROUP_ID = "io.delta"
DRIVER_ARTIFACT_ID = "delta-core_2.12"
DRIVER_VERSION = "0.8.0"


class SparkDeltaStorageConfig(StorageServiceConfig):
    type: str = wysdom.UserProperty(wysdom.SchemaConst('spark_delta'))


class SparkDeltaStorageService(
    SparkStorageService,
    Service[SparkDeltaStorageConfig, SparkStorageServiceOwner],
    register_as="spark_delta"
):

    spark_config_options: Dict[str, str] = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }

    spark_jars_packages: List[str] = [
        f"{DRIVER_GROUP_ID}:{DRIVER_ARTIFACT_ID}:{DRIVER_VERSION}"
    ]

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
            column_references: Dict[str, str],
            deleted_indicator: Optional[str] = None
    ):
        merge_sql_template = '''
            MERGE 
             INTO {{ target }} AS target
            USING {{ source }} AS source
               ON target.{{ key_column_name }}
                = source.{{ key_column_name }}
             {% if deleted_indicator %}
             WHEN MATCHED AND source.{{ deleted_indicator }} = 1 THEN DELETE
             {% endif %}
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
            column_references=column_references,
            deleted_indicator=deleted_indicator
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
