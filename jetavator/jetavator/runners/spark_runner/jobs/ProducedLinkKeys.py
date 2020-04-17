from typing import List

from jetavator.schema_registry import Satellite, Link

from .. import SparkSQLView, SparkJobABC, SparkRunnerABC


class ProducedLinkKeys(SparkSQLView, register_as='produced_link_keys'):
    name_template = 'produced_keys_{{link.full_name}}_{{satellite.full_name}}'
    sql_template = '''
        SELECT {{ link.key_column_name }}
               , array('sat_{{ satellite.name }}') AS key_source
               {% for alias in link.link_hubs.keys() %}
               , hub_{{alias}}_key
               {% endfor %}
          FROM vault_updates_{{satellite.full_name}}
        '''
    template_args = ['satellite', 'link']
    checkpoint = False
    global_view = False

    def __init__(
        self,
        runner: SparkRunnerABC,
        satellite: Satellite,
        link: Link
    ) -> None:
        super().__init__(runner, satellite, link)
        self.satellite = satellite
        self.link = link

    @property
    def dependencies(self) -> List[SparkJobABC]:
        return [self.runner.get_job('satellite_query', self.satellite)]
