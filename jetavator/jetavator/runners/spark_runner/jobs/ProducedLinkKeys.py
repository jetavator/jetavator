from __future__ import annotations

from typing import List

from jetavator.schema_registry import Satellite, Link

from .. import SparkSQLView, SparkRunnerABC


class ProducedLinkKeys(SparkSQLView, register_as='produced_link_keys'):
    sql_template = '''
        SELECT {{ job.link.key_column_name }}
               , array('sat_{{ job.satellite.name }}') AS key_source
               {% for alias in job.link.link_hubs.keys() %}
               , hub_{{alias}}_key
               {% endfor %}
          FROM vault_updates_{{job.satellite.full_name}}
        '''
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
    def name(self) -> str:
        return (
            'produced_keys'
            f'_{self.link.full_name}'
            f'_{self.satellite.full_name}'
        )

    @classmethod
    def keys_for_satellite(
        cls,
        runner: SparkRunnerABC,
        satellite: Satellite
    ) -> List[ProducedLinkKeys]:
        return [
            cls(runner, satellite, link)
            for link in satellite.produced_keys('link').values()
        ]

    @property
    def dependency_keys(self) -> List[str]:
        return [self.construct_job_key('satellite_query', self.satellite)]
