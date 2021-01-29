from __future__ import annotations

import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)

import sqlalchemy

from .. import SparkView
from jetavator.runners.jobs import CreateSource


def sqlalchemy_spark_type(sqlalchemy_type: sqlalchemy.types.TypeEngine):
    if isinstance(sqlalchemy_type, sqlalchemy.types.Binary):
        return BinaryType()
    elif isinstance(sqlalchemy_type, sqlalchemy.types.Boolean):
        return BooleanType()
    elif isinstance(sqlalchemy_type, sqlalchemy.types.Date):
        return DateType()
    elif isinstance(sqlalchemy_type, sqlalchemy.types.DateTime):
        return TimestampType()
    elif isinstance(sqlalchemy_type, sqlalchemy.types.Integer):
        return LongType()
    elif isinstance(sqlalchemy_type, sqlalchemy.types.String):
        return StringType()
    elif isinstance(sqlalchemy_type, sqlalchemy.types.Float):
        return DoubleType()


class SparkCreateSource(SparkView, CreateSource, register_as='create_source'):

    checkpoint = True
    global_view = False

    def execute_view(self) -> DataFrame:
        df = (
            self.spark
            .read
            .option("header", "true")
            .schema(self.spark_schema)
            .csv(self.source.csv_files)
        )
        return (
            df
            .withColumn(self.source.LOAD_TIMESTAMP_SYSTEM_COLUMN, self.load_timestamp_column(df))
            .withColumn(self.source.DELETED_INDICATOR_SYSTEM_COLUMN, self.deleted_indicator_column(df))
        )

    def deleted_indicator_column(self, df):
        return (
            df[self.source.deleted_indicator_column].cast(BooleanType())
            if self.source.deleted_indicator_column
            else lit(False)
        )

    def load_timestamp_column(self, df):
        return (
            df[self.source.load_timestamp_column].cast(TimestampType())
            if self.source.load_timestamp_column
            else lit(datetime.datetime.now())
        )

    @property
    def spark_schema(self):
        return StructType([
            StructField(k, sqlalchemy_spark_type(v.type.sqlalchemy_type), v.nullable)
            for k, v in self.source.columns.items()
        ])
