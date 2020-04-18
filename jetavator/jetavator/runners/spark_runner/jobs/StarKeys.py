from typing import List

from jetavator.schema_registry import SatelliteOwner, Satellite

from .. import SparkSQLView, SparkRunnerABC


class StarKeys(SparkSQLView, register_as='star_keys'):
    sql_template = '''
        SELECT {{ job.satellite_owner.key_column_name }},
               {% if job.satellite_owner.type == "link" %}
               {% for alias in job.satellite_owner.link_hubs.keys() %}
               first(hub_{{alias}}_key) AS hub_{{alias}}_key,
               {% endfor %}
               {% endif %}
               flatten(collect_set(key_source)) AS key_source
          FROM (
                {% for satellite in job.satellite_owner.satellites_containing_keys.values() %}
                SELECT
                       {{ job.satellite_owner.key_column_name }},
                       {% if job.satellite_owner.type == "link" %}
                       {% for alias in job.satellite_owner.link_hubs.keys() %}
                       hub_{{alias}}_key,
                       {% endfor %}
                       {% endif %}
                       key_source
                  FROM keys_{{job.satellite_owner.full_name}}_{{satellite.full_name}}

                {{ "UNION ALL" if not loop.last }}
                {% endfor %}
               ) AS keys
         GROUP
            BY {{ job.satellite_owner.key_column_name }}
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
        return f'keys_{self.satellite_owner.full_name}'

    def satellite_owner_output_keys(
        self,
        satellite: Satellite
    ) -> str:
        owner = self.satellite_owner
        if owner.name not in satellite.input_keys(owner.type):
            job_class = 'output_keys_from_satellite'
        elif owner.name not in satellite.produced_keys(owner.type):
            job_class = 'output_keys_from_dependencies'
        else:
            job_class = 'output_keys_from_both'
        return self.construct_job_key(job_class, satellite, owner)

    @property
    def dependency_keys(self) -> List[str]:
        return [
            self.satellite_owner_output_keys(satellite)
            for satellite
            in self.satellite_owner.satellites_containing_keys.values()
        ]
