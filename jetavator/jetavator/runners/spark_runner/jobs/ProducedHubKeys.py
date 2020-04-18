from __future__ import annotations

from typing import List

from jetavator.schema_registry import Satellite, Hub

from .. import SparkSQLView, SparkRunnerABC


class ProducedHubKeys(SparkSQLView, register_as='produced_hub_keys'):
    sql_template = '''
        {% if job.key_columns|length > 1 %}
        SELECT {{ job.hub.key_column_name }},
               collect_set(key_source) AS key_source
          FROM (
                {% for column in job.key_columns %}
                SELECT {{ column.name }} AS {{ job.hub.key_column_name }},
                       '{{ column.source }}' AS key_source
                  FROM vault_updates_{{job.satellite.full_name}}

                {{ "UNION ALL" if not loop.last }}
                {% endfor %}
               ) AS keys
         GROUP
            BY {{ job.hub.key_column_name }}
        {% else %}
        SELECT {{ job.key_columns[0].name }} AS {{ job.hub.key_column_name }},
               array('{{ job.key_columns[0].source }}') AS key_source
          FROM vault_updates_{{job.satellite.full_name}}
         GROUP
            BY {{ job.key_columns[0].name }}
        {% endif %}
        '''
    checkpoint = True
    global_view = False

    def __init__(
        self,
        runner: SparkRunnerABC,
        satellite: Satellite,
        hub: Hub
    ) -> None:
        super().__init__(runner, satellite, hub)
        self.satellite = satellite
        self.hub = hub

    @property
    def name(self) -> str:
        return (
            'produced_keys'
            f'_{self.hub.full_name}'
            f'_{self.satellite.full_name}'
        )

    @classmethod
    def keys_for_satellite(
        cls,
        runner: SparkRunnerABC,
        satellite: Satellite
    ) -> List[ProducedHubKeys]:
        return [
            cls(runner, satellite, hub)
            for hub in satellite.produced_keys('hub').values()
        ]

    # TODO: Type signature for ProducedHubKeys.key_columns
    @property
    def key_columns(self):
        return self.satellite.hub_key_columns[self.hub.name]

    @property
    def dependency_keys(self) -> List[str]:
        return [self.construct_job_key('satellite_query', self.satellite)]
