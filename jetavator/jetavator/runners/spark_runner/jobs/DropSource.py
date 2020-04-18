from typing import List

from jetavator.schema_registry import Source

from .. import SparkSQLJob, SparkRunnerABC


class DropSource(SparkSQLJob, register_as='drop_source'):
    sql_template = '''
        DROP TABLE source_{{job.source.name}}
        '''

    def __init__(self, runner: SparkRunnerABC, source: Source) -> None:
        super().__init__(runner, source)
        self.source = source

    @property
    def name(self) -> str:
        return f'drop_source_{self.source.name}'

    @property
    def dependency_keys(self) -> List[str]:
        return [
            self.construct_job_key('satellite_query', satellite)
            for satellite in self.source.dependent_satellites
        ]
