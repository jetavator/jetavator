from typing import List

from jetavator.schema_registry import Satellite

from .. import SparkSQLJob, SparkRunnerABC


class SerialiseSatellite(SparkSQLJob, register_as='serialise_satellite'):
    sql_template = '''
        INSERT
          INTO {{job.satellite.sql_model.table.name}}
        SELECT *
          FROM vault_updates_{{job.satellite.full_name}} AS source
        '''

    def __init__(self, runner: SparkRunnerABC, satellite: Satellite) -> None:
        super().__init__(runner, satellite)
        self.satellite = satellite

    @property
    def name(self) -> str:
        return f'serialise_sat_{self.satellite.name}'

    @property
    def dependency_keys(self) -> List[str]:
        return [self.construct_job_key('satellite_query', self.satellite)]
