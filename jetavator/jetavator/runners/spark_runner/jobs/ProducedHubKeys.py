from typing import List

from jetavator.schema_registry import Satellite, Hub

from .. import SparkSQLView, SparkJobABC, SparkRunnerABC


class ProducedHubKeys(SparkSQLView, register_as='produced_hub_keys'):
    name_template = 'produced_keys_{{hub.full_name}}_{{satellite.full_name}}'
    sql_template = '''
        {% if key_columns|length > 1 %}
        SELECT {{ hub.key_column_name }},
               collect_set(key_source) AS key_source
          FROM (
                {% for column in key_columns %}
                SELECT {{ column.name }} AS {{ hub.key_column_name }},
                       '{{ column.source }}' AS key_source
                  FROM vault_updates_{{satellite.full_name}}

                {{ "UNION ALL" if not loop.last }}
                {% endfor %}
               ) AS keys
         GROUP
            BY {{ hub.key_column_name }}
        {% else %}
        SELECT {{ key_columns[0].name }} AS {{ hub.key_column_name }},
               array('{{ key_columns[0].source }}') AS key_source
          FROM vault_updates_{{satellite.full_name}}
         GROUP
            BY {{ key_columns[0].name }}
        {% endif %}
        '''
    template_args = ['satellite', 'hub', 'key_columns']
    key_args = ['satellite', 'hub']
    checkpoint = True
    global_view = False

    def __init__(
        self,
        runner: SparkRunnerABC,
        satellite: Satellite,
        hub: Hub
    ) -> None:
        super().__init__(
            runner,
            satellite=satellite,
            hub=hub,
            key_columns=satellite.hub_key_columns[hub.name]
        )
        self.satellite = satellite
        self.hub = hub

    @property
    def dependencies(self) -> List[SparkJobABC]:
        return [self.runner.get_job('satellite_query', self.satellite)]
