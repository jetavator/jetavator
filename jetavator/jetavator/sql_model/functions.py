from sqlalchemy.types import *
from sqlalchemy.sql.expression import func, literal_column, cast
from sqlalchemy.sql.functions import coalesce


def hash_value(value_to_hash):
    return func.md5(value_to_hash)


def hash_keygen(column):
    return hash_value(func.UPPER(func.LTRIM(func.RTRIM(column))))


def hash_record(table, deleted_ind_name, column_names):
        value_to_hash = cast(
            coalesce(table.c[deleted_ind_name], literal_column("0")),
            String(None)
        )
        for column_name in column_names:
            value_to_hash = value_to_hash.concat(
                coalesce(
                    cast(table.c[column_name], String(None)),
                    literal_column("''")))
        return hash_value(value_to_hash)
