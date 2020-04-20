from typing import List

from jetavator.schema_registry import Source

from .. import SparkSQLJob, SparkRunner, SparkJob


class DropSource(SparkSQLJob, register_as='drop_source'):
    """
    Drop the temporary table for a source CSV file.

    :param runner: The `SparkRunner` that created this object.
    :param source: The `Source` object containing the source definitions.
    """

    sql_template = '''
        DROP TABLE source_{{job.source.name}}
        '''

    def __init__(self, runner: SparkRunner, source: Source) -> None:
        super().__init__(runner, source)
        self.source = source

    @property
    def name(self) -> str:
        return f'drop_source_{self.source.name}'

    @property
    def dependencies(self) -> List[SparkJob]:
        return [
            self.runner.get_job('satellite_query', satellite)
            for satellite in self.source.dependent_satellites
        ]
