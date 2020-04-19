from __future__ import annotations

from typing import List

from abc import ABC

from jetavator.runners.spark_runner import SparkSQLView, SparkRunnerABC
from jetavator.schema_registry import Satellite, SatelliteOwner


class ProducedKeys(SparkSQLView, ABC):
    checkpoint = False
    global_view = False

    def __init__(
        self,
        runner: SparkRunnerABC,
        satellite: Satellite,
        satellite_owner: SatelliteOwner
    ) -> None:
        super().__init__(runner, satellite, satellite_owner)
        self.satellite = satellite
        self.satellite_owner = satellite_owner

    @classmethod
    def keys_for_satellite(
        cls,
        runner: SparkRunnerABC,
        satellite: Satellite
    ) -> List[ProducedKeys]:
        return [
            cls.registered_subclass_instance(
                f'produced_{satellite_owner.registered_name}_keys',
                runner,
                satellite,
                satellite_owner
            )
            for satellite_owner in satellite.produced_keys()
        ]

    @property
    def name(self) -> str:
        return (
            'produced_keys'
            f'_{self.satellite_owner.full_name}'
            f'_{self.satellite.full_name}'
        )

    @property
    def dependency_keys(self) -> List[str]:
        return [self.construct_job_key('satellite_query', self.satellite)]


class ProducedLinkKeys(ProducedKeys, register_as='produced_link_keys'):
    sql_template = '''
        SELECT {{ job.satellite_owner.key_column_name }}
               , array('sat_{{ job.satellite.name }}') AS key_source
               {% for alias in job.satellite_owner.link_hubs.keys() %}
               , hub_{{alias}}_key
               {% endfor %}
          FROM vault_updates_{{job.satellite.full_name}}
        '''


class ProducedHubKeys(ProducedKeys, register_as='produced_hub_keys'):
    sql_template = '''
        {% if job.key_columns|length > 1 %}
        SELECT {{ job.satellite_owner.key_column_name }},
               collect_set(key_source) AS key_source
          FROM (
                {% for column in job.key_columns %}
                SELECT {{ column.name }} AS {{ job.satellite_owner.key_column_name }},
                       '{{ column.source }}' AS key_source
                  FROM vault_updates_{{job.satellite.full_name}}

                {{ "UNION ALL" if not loop.last }}
                {% endfor %}
               ) AS keys
         GROUP
            BY {{ job.satellite_owner.key_column_name }}
        {% else %}
        SELECT {{ job.key_columns[0].name }} AS {{ job.satellite_owner.key_column_name }},
               array('{{ job.key_columns[0].source }}') AS key_source
          FROM vault_updates_{{job.satellite.full_name}}
         GROUP
            BY {{ job.key_columns[0].name }}
        {% endif %}
        '''

    # TODO: Type signature for ProducedHubKeys.key_columns
    @property
    def key_columns(self):
        return self.satellite.hub_key_columns[self.satellite_owner.name]
