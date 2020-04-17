from __future__ import annotations

from jetavator.schema_registry import Source

from .. import SparkSQLJob, SparkRunnerABC


class CreateSource(SparkSQLJob, register_as='create_source'):
    name_template = 'create_source_{{source.name}}'
    sql_template = '''
        {{source_ddl}}
        {% if csv_path is defined %}
        USING csv
        OPTIONS (
            path "{{csv_path}}",
            header "true",
            inferSchema "false"
        )
        {% endif %}
        '''
    template_args = ['source', 'source_ddl', 'csv_path']
    key_args = ['source']

    def __init__(self, runner: SparkRunnerABC, source: Source) -> None:
        source_ddl = runner.compute_service.compile_sqlalchemy(
            source.sql_model
            .create_table(source.sql_model.table)[0]
        )
        if runner.compute_service.source_csv_exists(source):
            super().__init__(
                runner,
                source,
                source_ddl=source_ddl.replace(
                    'CREATE TABLE', 'CREATE TEMPORARY TABLE'),
                csv_path=runner.compute_service.csv_file_path(source)
            )
        else:
            super().__init__(
                runner,
                source,
                source_ddl=source_ddl.replace(
                    'CREATE TABLE', 'CREATE TABLE IF NOT EXISTS'),
            )
            self.logger.warning(f'CSV file not found for source: {source}')
