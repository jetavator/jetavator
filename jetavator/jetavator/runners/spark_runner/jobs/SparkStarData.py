from .. import SparkSQLView
from jetavator.runners.jobs import StarData


class SparkStarData(SparkSQLView, StarData, register_as='star_data'):

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
