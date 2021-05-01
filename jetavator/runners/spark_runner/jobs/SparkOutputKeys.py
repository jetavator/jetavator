from __future__ import annotations

from .. import SparkSQLView
from jetavator.runners.jobs import OutputKeys


class SparkOutputKeys(SparkSQLView, OutputKeys, register_as='output_keys'):

    checkpoint = True
    global_view = False

    @property
    def sql_template(self) -> str:
        if not self.owner_in_input_keys:
            return '''
                SELECT * 
                  FROM {{ job.produced_keys_job.name }}
            '''
        if not self.owner_in_produced_keys:
            return '''
                SELECT * 
                  FROM {{ job.input_keys_job.name }}
            '''
        else:
            return '''
                SELECT COALESCE(input_keys.{{ job.satellite_owner.key_column_name }},
                                produced_keys.{{ job.satellite_owner.key_column_name }})
                             AS {{ job.satellite_owner.key_column_name }},
                       {% if job.satellite_owner.type == "link" %}
                       {% for alias in job.satellite_owner.hubs.keys() %}
                       COALESCE(input_keys.hub_{{ alias }}_key,
                                produced_keys.hub_{{ alias }}_key)
                             AS hub_{{alias}}_key,
                       {% endfor %}
                       {% endif %}
                       CONCAT(COALESCE(input_keys.key_source, array()),
                              COALESCE(produced_keys.key_source, array()))
                           AS key_source
                  FROM {{ job.input_keys_job.name }} AS input_keys
                       FULL JOIN {{ job.produced_keys_job.name }} AS produced_keys
                              ON input_keys.{{ job.satellite_owner.key_column_name }}
                               = produced_keys.{{ job.satellite_owner.key_column_name }}
        
                '''
