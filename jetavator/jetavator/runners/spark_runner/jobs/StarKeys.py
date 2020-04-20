from typing import List

from jetavator.schema_registry import SatelliteOwner

from .. import SparkSQLView, SparkRunner, SparkJob


class StarKeys(SparkSQLView, register_as='star_keys'):
    """
    Collects all the key values for any `Satellite` row that has been created,
    updated or deleted for this particular `Hub` or `Link`.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite_owner: The `Hub` or `Link` object that is being used to create
                            a Dimension or Fact table, respectively.
    """

    sql_template = '''
        SELECT {{ job.satellite_owner.key_column_name }},
               {% if job.satellite_owner.type == "link" %}
               {% for alias in job.satellite_owner.link_hubs.keys() %}
               first(hub_{{alias}}_key) AS hub_{{alias}}_key,
               {% endfor %}
               {% endif %}
               flatten(collect_set(key_source)) AS key_source
          FROM (
                {% for dep in job.dependencies %}
                SELECT
                       {{ job.satellite_owner.key_column_name }},
                       {% if job.satellite_owner.type == "link" %}
                       {% for alias in job.satellite_owner.link_hubs.keys() %}
                       hub_{{alias}}_key,
                       {% endfor %}
                       {% endif %}
                       key_source
                  FROM {{ dep.name }}
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
            runner: SparkRunner,
            satellite_owner: SatelliteOwner
    ) -> None:
        super().__init__(runner, satellite_owner)
        self.satellite_owner = satellite_owner

    @property
    def name(self) -> str:
        return f'keys_{self.satellite_owner.full_name}'

    @property
    def dependencies(self) -> List[SparkJob]:
        return [
            self.runner.get_job('output_keys', satellite, self.satellite_owner)
            for satellite
            in self.satellite_owner.satellites_containing_keys.values()
        ]
