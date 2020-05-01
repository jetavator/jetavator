from typing import List

from jetavator.schema_registry import SatelliteOwner

from .. import SparkSQLView, SparkRunner, SparkJob


class StarData(SparkSQLView, register_as='star_data'):
    """
    Computes the created, updated or deleted rows for the star schema table
    for this particular `Hub` or `Link`.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite_owner: The `Hub` or `Link` object that is being used to create
                            a Dimension or Fact table, respectively.
    """

    sql_template = '''
        SELECT keys.{{job.satellite_owner.key_column_name}}

               {% if job.satellite_owner.type == "link" %}
               {% for alias in job.satellite_owner.hubs.keys() %}
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

          FROM {{ job.star_keys_job.name }} AS keys
          {% for query_job in job.satellite_query_jobs %}
          LEFT
          JOIN {{ query_job.name }} AS {{ query_job.satellite.name }}
            ON {{ query_job.satellite.name }}.{{ job.satellite_owner.key_column_name }} =
               keys.{{ job.satellite_owner.key_column_name }}
          {% endfor %}
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
        return f'updates_{self.satellite_owner.star_table_name}'

    @property
    def star_keys_job(self) -> SparkJob:
        """
        :return: The `StarKeys` job that contains the updated keys.
        """
        return self.runner.get_job('star_keys', self.satellite_owner)

    @property
    def satellite_query_jobs(self) -> List[SparkJob]:
        """
        :return: A list of the `SatelliteQuery` jobs that contain the updated data.
        """
        return [
            self.runner.get_job('satellite_query', satellite)
            for satellite in self.satellite_owner.star_satellites.values()
        ]

    @property
    def dependencies(self) -> List[SparkJob]:
        return [
            self.star_keys_job,
            *self.satellite_query_jobs
        ]
