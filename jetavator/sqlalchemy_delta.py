"""
Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license, and

https://github.com/dropbox/PyHive
which is released under the Apache 2.0 license
"""

import datetime

import re
from sqlalchemy import exc
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.sql.compiler import SQLCompiler

import collections


class UniversalSet(object):
    """set containing everything"""

    def __contains__(self, item):
        return True


class ParamEscaper(object):
    _DATE_FORMAT = "%Y-%m-%d"
    _TIME_FORMAT = "%H:%M:%S.%f"
    _DATETIME_FORMAT = "{} {}".format(_DATE_FORMAT, _TIME_FORMAT)

    def escape_args(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return tuple(self.escape_item(x) for x in parameters)
        else:
            raise exc.ProgrammingError(
                "Unsupported param format: {}".format(parameters)
            )

    def escape_number(self, item):
        return item

    def escape_string(self, item):
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        # This is good enough when backslashes are literal, newlines are just followed, and the way
        # to escape a single quote is to put two single quotes.
        # (i.e. only special character is single quote)
        return "'{}'".format(item.replace("'", "''"))

    def escape_sequence(self, item):
        l = map(str, map(self.escape_item, item))
        return "(" + ",".join(l) + ")"

    def escape_datetime(self, item, format, cutoff=0):
        dt_str = item.strftime(format)
        formatted = dt_str[:-cutoff] if cutoff and format.endswith(".%f") else dt_str
        return "'{}'".format(formatted)

    def escape_item(self, item):
        if item is None:
            return "NULL"
        elif isinstance(item, (int, float)):
            return self.escape_number(item)
        elif isinstance(item, str):
            return self.escape_string(item)
        elif isinstance(item, collections.Iterable):
            return self.escape_sequence(item)
        elif isinstance(item, datetime.datetime):
            return self.escape_datetime(item, self._DATETIME_FORMAT)
        elif isinstance(item, datetime.date):
            return self.escape_datetime(item, self._DATE_FORMAT)
        else:
            raise exc.ProgrammingError("Unsupported object {}".format(item))


class HiveParamEscaper(ParamEscaper):
    def escape_string(self, item):
        # backslashes and single quotes need to be escaped
        # TODO verify against parser
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode("utf-8")
        return "'{}'".format(
            item.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
        )


class HiveIdentifierPreparer(compiler.IdentifierPreparer):
    # Just quote everything to make things simpler / easier to upgrade
    reserved_words = UniversalSet()

    def __init__(self, dialect):
        super(HiveIdentifierPreparer, self).__init__(
            dialect,
            initial_quote="`",
        )


class HiveCompiler(SQLCompiler):
    def visit_concat_op_binary(self, binary, operator, **kw):
        return "concat(%s, %s)" % (
            self.process(binary.left),
            self.process(binary.right),
        )

    def visit_insert(self, *args, **kwargs):
        result = super(HiveCompiler, self).visit_insert(*args, **kwargs)
        # Massage the result into Hive's format
        #   INSERT INTO `pyhive_test_database`.`test_table` (`a`) SELECT ...
        #   =>
        #   INSERT INTO TABLE `pyhive_test_database`.`test_table` SELECT ...
        regex = r"^(INSERT INTO) ([^\s]+) \([^\)]*\)"
        assert re.search(regex, result), "Unexpected visit_insert result: {}".format(
            result
        )
        return re.sub(regex, r"\1 TABLE \2", result)

    def visit_column(self, *args, **kwargs):
        result = super(HiveCompiler, self).visit_column(*args, **kwargs)
        dot_count = result.count(".")
        assert dot_count in (0, 1, 2), "Unexpected visit_column result {}".format(
            result
        )
        if dot_count == 2:
            # we have something of the form schema.table.column
            # hive doesn't like the schema in front, so chop it out
            result = result[result.index(".") + 1 :]
        return result

    def visit_char_length_func(self, fn, **kw):
        return "length{}".format(self.function_argspec(fn, **kw))


class HiveTypeCompiler(compiler.GenericTypeCompiler):
    def visit_INTEGER(self, type_, **kwargs):
        return "INT"

    def visit_NUMERIC(self, type_, **kwargs):
        return "DECIMAL"

    def visit_CHAR(self, type_, **kwargs):
        return "STRING"

    def visit_VARCHAR(self, type_, **kwargs):
        return "STRING"

    def visit_NCHAR(self, type_, **kwargs):
        return "STRING"

    def visit_TEXT(self, type_, **kwargs):
        return "STRING"

    def visit_CLOB(self, type_, **kwargs):
        return "STRING"

    def visit_BLOB(self, type_, **kwargs):
        return "BINARY"

    def visit_TIME(self, type_, **kwargs):
        return "TIMESTAMP"

    def visit_DATE(self, type_, **kwargs):
        return "TIMESTAMP"

    def visit_DATETIME(self, type_, **kwargs):
        return "TIMESTAMP"


class HiveDDLCompiler(compiler.DDLCompiler):
    def visit_create_column(self, *args, **kwargs):
        result = super().visit_create_column(*args, **kwargs)
        result = result.replace(" NOT NULL", "")
        result = result.replace(" PRIMARY KEY", "")
        result = result.replace(" FOREIGN KEY", "")
        return result

    # noinspection PyMethodMayBeStatic
    def create_table_constraints(self, table, _include_foreign_key_constraints=None):
        return None

    def __str__(self):
        result = super().__str__()
        result = result.replace("%%", "%")
        return result


class HiveDialect(default.DefaultDialect):

    name = b"hive"
    preparer = HiveIdentifierPreparer
    statement_compiler = HiveCompiler
    ddl_compiler = HiveDDLCompiler
    type_compiler = HiveTypeCompiler
    supports_views = True
    supports_alter = True
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_multivalues_insert = True
    supports_sane_rowcount = False

    def get_columns(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError

    def get_table_names(self, connection, schema=None, **kw):
        raise NotImplementedError

    def get_temp_table_names(self, connection, schema=None, **kw):
        raise NotImplementedError

    def get_view_names(self, connection, schema=None, **kw):
        raise NotImplementedError

    def get_temp_view_names(self, connection, schema=None, **kw):
        raise NotImplementedError

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        raise NotImplementedError

    def get_indexes(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError

    def has_table(self, connection, table_name, schema=None):
        raise NotImplementedError

    def has_sequence(self, connection, sequence_name, schema=None):
        raise NotImplementedError

    def _get_server_version_info(self, connection):
        raise NotImplementedError

    def _get_default_schema_name(self, connection):
        raise NotImplementedError

    def do_begin_twophase(self, connection, xid):
        raise NotImplementedError

    def do_prepare_twophase(self, connection, xid):
        raise NotImplementedError

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_recover_twophase(self, connection):
        raise NotImplementedError

    def set_isolation_level(self, dbapi_conn, level):
        raise NotImplementedError

    def get_isolation_level(self, dbapi_conn):
        raise NotImplementedError


# noinspection PyAbstractClass
class DeltaCompiler(HiveCompiler):

    # noinspection PyMethodMayBeStatic
    def render_literal_value(self, value, type_):
        return str(HiveParamEscaper().escape_item(value))

    def __str__(self):
        result = super().__str__()
        result = result.replace("%%", "%")
        return result


class DeltaDDLCompiler(HiveDDLCompiler):
    def visit_create_table(self, *args, **kwargs):
        result = super().visit_create_table(*args, **kwargs)
        # TODO: Some tables need to be USING CSV and/or include LOCATION
        result += "\nUSING DELTA"
        return result


class DeltaTypeCompiler(HiveTypeCompiler):

    # noinspection PyMethodMayBeStatic,PyPep8Naming
    def visit_FLOAT(self, type_, **kwargs):
        return "DOUBLE"


# noinspection PyAbstractClass
class DeltaDialect(HiveDialect):
    name = b"delta"
    statement_compiler = DeltaCompiler
    ddl_compiler = DeltaDDLCompiler
    type_compiler = DeltaTypeCompiler
