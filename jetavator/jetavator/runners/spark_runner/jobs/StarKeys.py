from typing import List

from jetavator.schema_registry import SatelliteOwner

from .. import SparkSQLView, SparkJobABC, SparkRunnerABC


class StarKeys(SparkSQLView, register_as='star_keys'):
    name_template = 'keys_{{satellite_owner.full_name}}'
    sql_template = '''
        SELECT {{ satellite_owner.key_column_name }},
               {% if satellite_owner.type == "link" %}
               {% for alias in satellite_owner.link_hubs.keys() %}
               first(hub_{{alias}}_key) AS hub_{{alias}}_key,
               {% endfor %}
               {% endif %}
               flatten(collect_set(key_source)) AS key_source
          FROM (
                {% for satellite in satellite_owner.satellites_containing_keys.values() %}
                SELECT
                       {{ satellite_owner.key_column_name }},
                       {% if satellite_owner.type == "link" %}
                       {% for alias in satellite_owner.link_hubs.keys() %}
                       hub_{{alias}}_key,
                       {% endfor %}
                       {% endif %}
                       key_source
                  FROM keys_{{satellite_owner.full_name}}_{{satellite.full_name}}

                {{ "UNION ALL" if not loop.last }}
                {% endfor %}
               ) AS keys
         GROUP
            BY {{ satellite_owner.key_column_name }}
        '''
    template_args = ['satellite_owner']
    checkpoint = True
    global_view = False

    def __init__(
        self,
        runner: SparkRunnerABC,
        satellite_owner: SatelliteOwner
    ) -> None:
        super().__init__(runner, satellite_owner)
        self.satellite_owner = satellite_owner

    @property
    def dependencies(self) -> List[SparkJobABC]:
        return [
            self.runner.satellite_owner_output_keys(
                satellite,
                self.satellite_owner,
                self.satellite_owner.type
            )
            for satellite
            in self.satellite_owner.satellites_containing_keys.values()
        ]
