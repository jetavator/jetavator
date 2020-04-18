from typing import List

from jetavator.schema_registry import SatelliteOwner

from .. import SparkSQLView, SparkRunnerABC


class StarData(SparkSQLView, register_as='star_data'):
    sql_template = '''
        SELECT keys.{{job.satellite_owner.key_column_name}}

               {% if job.satellite_owner.type == "link" %}
               {% for alias in job.satellite_owner.link_hubs.keys() %}
               , keys.hub_{{alias}}_key
               {% endfor %}
               {% endif %}

               , keys.key_source

               {% for satellite in job.satellite_owner.star_satellites.values() %}
               , array_contains(
                    keys.key_source,
                    '{{satellite.full_name}}'
                 ) AS update_ind_{{satellite.name}}
               {% endfor %}

               {% if job.satellite_owner.star_satellites.values() | length > 1 %}
               , (LEAST(
                   {% for satellite in job.satellite_owner.star_satellites.values() %}
                   {{satellite.name}}.sat_deleted_ind{{"," if not loop.last}}
                   {% endfor %}
               ) == 1) AS deleted_ind
               {% elif job.satellite_owner.star_satellites.values() | length == 1 %}
               {% for satellite in job.satellite_owner.star_satellites.values() %}
               , ({{satellite.name}}.sat_deleted_ind == 1) AS deleted_ind
               {% endfor %}
               {% else %}
               , false AS deleted_ind
               {% endif %}

               {% for satellite in job.satellite_owner.star_satellites.values() %}
               {% for column in satellite.columns.keys() %}
               , {{satellite.name}}.{{column}}
               {% endfor %}
               {% endfor %}

          FROM keys_{{job.satellite_owner.full_name}} AS keys
          {% for satellite in job.satellite_owner.star_satellites.values() %}
          LEFT
          JOIN vault_updates_{{satellite.full_name}} AS {{satellite.name}}
            ON {{satellite.name}}.{{satellite.parent.key_column_name}} =
               keys.{{satellite.parent.key_column_name}}
          {% endfor %}
        '''
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
    def name(self) -> str:
        return f'updates_{self.satellite_owner.sql_model.star_table_name}'

    @property
    def dependency_keys(self) -> List[str]:
        return [
            self.construct_job_key('star_keys', self.satellite_owner),
            *[
                self.construct_job_key('satellite_query', satellite)
                for satellite in self.satellite_owner.star_satellites.values()
            ]
        ]
