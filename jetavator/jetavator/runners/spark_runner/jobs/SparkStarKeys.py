from .. import SparkSQLView
from jetavator.runners.jobs import StarKeys


class SparkStarKeys(SparkSQLView, StarKeys, register_as='star_keys'):

    sql_template = '''
        SELECT {{ job.satellite_owner.key_column_name }},
               {% if job.satellite_owner.type == "link" %}
               {% for alias in job.satellite_owner.hubs.keys() %}
               first(hub_{{alias}}_key) AS hub_{{alias}}_key,
               {% endfor %}
               {% endif %}
               flatten(collect_set(key_source)) AS key_source
          FROM (
                {% for dep in job.dependencies %}
                SELECT
                       {{ job.satellite_owner.key_column_name }},
                       {% if job.satellite_owner.type == "link" %}
                       {% for alias in job.satellite_owner.hubs.keys() %}
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
