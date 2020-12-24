from sqlalchemy.sql import compiler
from sqlalchemy import Table, MetaData

from pyhive.sqlalchemy_hive import HiveDialect, HiveCompiler, HiveTypeCompiler

from pyhive.hive import HiveParamEscaper


class DeltaCompiler(HiveCompiler):

    def render_literal_value(self, value, type_):
        return str(HiveParamEscaper().escape_item(value))

    def __str__(self):
        result = super().__str__()
        result = result.replace('%%', '%')
        return result


class HiveDDLCompiler(compiler.DDLCompiler):

    def visit_create_column(self, *args, **kwargs):
        result = super().visit_create_column(*args, **kwargs)
        result = result.replace(' NOT NULL', '')
        result = result.replace(' PRIMARY KEY', '')
        result = result.replace(' FOREIGN KEY', '')
        return result

    def create_table_constraints(
        self, table, _include_foreign_key_constraints=None
    ):
        return None

    def __str__(self):
        result = super().__str__()
        result = result.replace('%%', '%')
        return result


class DeltaDDLCompiler(HiveDDLCompiler):

    def visit_create_table(self, *args, **kwargs):
        result = super().visit_create_table(*args, **kwargs)
        # TODO: Some tables need to be USING CSV and/or include LOCATION
        result += '\nUSING DELTA'
        return result


class DeltaTypeCompiler(HiveTypeCompiler):

    def visit_FLOAT(self, type_, **kwargs):
        return 'DOUBLE'


class HiveWithDDLDialect(HiveDialect):
    name = b'hive_with_ddl'
    statement_compiler = HiveCompiler
    ddl_compiler = HiveDDLCompiler
    type_compiler = HiveTypeCompiler


class DeltaDialect(HiveDialect):
    name = b'delta'
    statement_compiler = DeltaCompiler
    ddl_compiler = DeltaDDLCompiler
    type_compiler = DeltaTypeCompiler
