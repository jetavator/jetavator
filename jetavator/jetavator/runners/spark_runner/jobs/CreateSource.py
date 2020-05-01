from __future__ import annotations

from lazy_property import LazyProperty

from jetavator.schema_registry import Source

from .. import SparkSQLJob, SparkRunner


class CreateSource(SparkSQLJob, register_as='create_source'):
    """
    Creates a temporary table for a source CSV file if the file exists,
    or a placeholder empty table if it does not.

    :param runner: The `SparkRunner` that created this object.
    :param source: The `Source` object containing the source definitions.
    """

    sql_template = '''
        {{ job.source_ddl }}
        {% if job.csv_exists %}
        USING csv
        OPTIONS (
            path "{{ job.csv_path }}",
            header "true",
            inferSchema "false"
        )
        {% endif %}
        '''

    def __init__(self, runner: SparkRunner, source: Source) -> None:
        super().__init__(runner, source)
        self.source = source
        if not self.csv_exists:
            self.logger.warning(f'CSV file not found for source: {source}')

    @property
    def name(self) -> str:
        return f'create_source_{self.source.name}'

    @property
    def source_ddl(self) -> str:
        """
        :return: The Spark SQL DDL statement to create this `Source` table.
        """
        ddl = self.runner.compute_service.compile_sqlalchemy(
            self.source.create_table_statement)
        if self.csv_path:
            return ddl.replace('CREATE TABLE', 'CREATE TEMPORARY TABLE')
        else:
            return ddl.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')

    @LazyProperty
    def csv_exists(self) -> bool:
        """
        :return: Returns True if the supplied `Source` CSV file exists on disk.
        """
        return self.runner.compute_service.source_csv_exists(self.source)

    @property
    def csv_path(self) -> str:
        """
        :return: Returns the path to the supplied `Source` CSV file.
        """
        return self.runner.compute_service.csv_file_path(self.source)
