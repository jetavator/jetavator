from typing import Iterable

from sqlalchemy import Table
from sqlalchemy.types import *
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.expression import func, literal_column, cast
from sqlalchemy.sql.functions import FunctionElement, coalesce


def hash_value(value_to_hash: ColumnElement) -> FunctionElement:
    return func.md5(value_to_hash)


def hash_keygen(column: ColumnElement) -> FunctionElement:
    return hash_value(func.UPPER(func.LTRIM(func.RTRIM(column))))


def hash_record(
        table: Table,
        deleted_ind_name: str,
        column_names: Iterable[str]
) -> FunctionElement:
    value_to_hash = cast(
        coalesce(table.c[deleted_ind_name], literal_column("FALSE")),
        String()
    )
    for column_name in column_names:
        value_to_hash = value_to_hash.concat(
            coalesce(
                cast(table.c[column_name], String()),
                literal_column("''")))
    return hash_value(value_to_hash)
