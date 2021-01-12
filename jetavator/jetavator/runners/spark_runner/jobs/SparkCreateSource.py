from __future__ import annotations

from .. import SparkSQLJob
from jetavator.runners.jobs import CreateSource


class SparkCreateSource(SparkSQLJob, CreateSource, register_as='create_source'):

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

    @property
    def source_ddl(self) -> str:
        """
        :return: The Spark SQL DDL statement to create this `Source` table.
        """
        ddl = self.runner.compute_service.compile_hive(
            self.source.create_table_statement)
        if self.csv_path:
            self.logger.info(f"Using CSV path {self.csv_path} for {self.name}")
            return ddl.replace('CREATE TABLE', 'CREATE TEMPORARY VIEW')
        else:
            # TODO: Check if this still works in Spark 3.0
            self.logger.info(f"No CSV path specified so creating empty placeholder table for {self.name}")
            return ddl.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
