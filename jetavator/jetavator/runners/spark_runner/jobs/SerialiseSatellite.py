from typing import List

from jetavator.schema_registry import Satellite

from .. import SparkJobABC, SparkSQLJob, SparkRunnerABC


class SerialiseSatellite(SparkSQLJob, register_as='serialise_satellite'):
    name_template = 'serialise_sat_{{satellite.name}}'
    sql_template = '''
        INSERT
          INTO {{satellite.sql_model.table.name}}
        SELECT *
          FROM vault_updates_{{satellite.full_name}} AS source
        '''
    template_args = ['satellite']

    def __init__(self, runner: SparkRunnerABC, satellite: Satellite) -> None:
        super().__init__(runner, satellite)
        self.satellite = satellite

    @property
    def dependencies(self) -> List[SparkJobABC]:
        return [self.runner.get_job('satellite_query', self.satellite)]
