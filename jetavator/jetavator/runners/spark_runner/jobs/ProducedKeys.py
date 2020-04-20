from __future__ import annotations

from typing import List

from abc import ABC

from jetavator.schema_registry import Satellite, SatelliteOwner, HubKeyColumn

from .. import SparkSQLView, SparkRunner, SparkJob


class ProducedKeys(SparkSQLView, ABC):
    """
    Computes a DataFrame containing the `Hub` or `Link` key values for any
    satellite row that has been created, updated or deleted by this satellite
    or its dependencies.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite:       The `Satellite` object that is passing the updated
                            keys to its dependencies.
    :param satellite_owner: A `Hub` or `Link` describing the grain of the updated
                            keys.
    """

    checkpoint = False
    global_view = False

    def __init__(
            self,
            runner: SparkRunner,
            satellite: Satellite,
            satellite_owner: SatelliteOwner
    ) -> None:
        super().__init__(runner, satellite, satellite_owner)
        self.satellite = satellite
        self.satellite_owner = satellite_owner

    @classmethod
    def keys_for_satellite(
            cls,
            runner: SparkRunner,
            satellite: Satellite
    ) -> List[ProducedKeys]:
        """
        Generate an `InputKeys` job for any `Hub` or `Link`, if that Hub
        or Link can have keys generated for it by this satellite.

        :param runner:    The `SparkRunner` that is creating these objects.
        :param satellite: The `Satellite` to search for output keys for.
        :return:          A list of `OutputKeys` jobs containing output keys
                          for all relevant `Hub`s and `Link`s.
        """
        return [
            cls.registered_subclass_instance(
                f'produced_{satellite_owner.registered_name}_keys',
                runner,
                satellite,
                satellite_owner
            )
            for satellite_owner in satellite.produced_keys()
        ]

    @property
    def name(self) -> str:
        return (
            'produced_keys'
            f'_{self.satellite_owner.full_name}'
            f'_{self.satellite.full_name}'
        )

    @property
    def satellite_query_job(self) -> SparkJob:
        """
        :return: The `SatelliteQuery` job that contains the updated data.
        """
        return self.runner.get_job('satellite_query', self.satellite)

    @property
    def dependencies(self) -> List[SparkJob]:
        return [self.satellite_query_job]


class ProducedLinkKeys(ProducedKeys, register_as='produced_link_keys'):
    """
    Computes a DataFrame containing the `Link` key values for any
    satellite row that has been created, updated or deleted by this satellite
    or its dependencies.
    """

    sql_template = '''
        SELECT {{ job.satellite_owner.key_column_name }}
               , array('sat_{{ job.satellite.name }}') AS key_source
               {% for alias in job.satellite_owner.link_hubs.keys() %}
               , hub_{{alias}}_key
               {% endfor %}
          FROM {{ job.satellite_query_job.name }}
        '''


class ProducedHubKeys(ProducedKeys, register_as='produced_hub_keys'):
    """
    Computes a DataFrame containing the `Hub` key values for any
    satellite row that has been created, updated or deleted by this satellite
    or its dependencies.
    """

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

    @property
    def key_columns(self) -> List[HubKeyColumn]:
        """
        :return: A list of the `HubKeyColumn`s for this particular `Hub`.
        """
        return self.satellite.hub_key_columns[self.satellite_owner.name]
