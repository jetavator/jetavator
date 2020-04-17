from typing import List

from jetavator.schema_registry import Source

from .. import SparkSQLJob, SparkJobABC, SparkRunnerABC


class DropSource(SparkSQLJob, register_as='drop_source'):
    name_template = 'drop_source_{{source.name}}'
    sql_template = '''
        DROP TABLE source_{{source.name}}
        '''
    template_args = ['source']
    key_args = ['source']

    def __init__(self, runner: SparkRunnerABC, source: Source) -> None:
        super().__init__(runner, source)
        self.source = source

    @property
    def dependencies(self) -> List[SparkJobABC]:
        return [
            self.runner.get_job('satellite_query', satellite)
            for satellite in self.source.dependent_satellites
        ]
