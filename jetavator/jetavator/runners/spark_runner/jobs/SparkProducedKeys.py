from __future__ import annotations

from abc import ABC

from .. import SparkSQLView
from jetavator.runners.jobs import ProducedKeys, ProducedHubKeys, ProducedLinkKeys


class SparkProducedKeys(SparkSQLView, ProducedKeys, ABC, register_as='produced_keys'):

    checkpoint = False
    global_view = False


class SparkProducedLinkKeys(SparkProducedKeys, ProducedLinkKeys, register_as='produced_link_keys'):

    sql_template = '''
        SELECT {{ job.satellite_owner.key_column_name }}
               , array('sat_{{ job.satellite.name }}') AS key_source
               {% for alias in job.satellite_owner.hubs.keys() %}
               , hub_{{alias}}_key
               {% endfor %}
          FROM {{ job.satellite_query_job.name }}
        '''


class SparkProducedHubKeys(SparkProducedKeys, ProducedHubKeys, register_as='produced_hub_keys'):

    sql_template = '''  
        {% if job.key_columns|length > 1 %}
        SELECT {{ job.satellite_owner.key_column_name }},
               collect_set(key_source) AS key_source
          FROM (
                {% for column in job.key_columns %}
                SELECT {{ column.name }} AS {{ job.satellite_owner.key_column_name }},
                       '{{ column.source }}' AS key_source
                  FROM {{ job.satellite_query_job.name }}

                {{ "UNION ALL" if not loop.last }}
                {% endfor %}
               ) AS keys
         GROUP
            BY {{ job.satellite_owner.key_column_name }}
        {% else %}
        SELECT {{ job.key_columns[0].name }} AS {{ job.satellite_owner.key_column_name }},
               array('{{ job.key_columns[0].source }}') AS key_source
          FROM {{ job.satellite_query_job.name }}
         GROUP
            BY {{ job.key_columns[0].name }}
        {% endif %}
        '''
