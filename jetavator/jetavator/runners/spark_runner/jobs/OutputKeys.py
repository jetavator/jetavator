from __future__ import annotations

from abc import ABC
from typing import List

from lazy_property import LazyProperty

from jetavator.schema_registry import Satellite, SatelliteOwner

from .. import SparkSQLView, SparkRunner, SparkJob


class OutputKeys(SparkSQLView, ABC, register_as='output_keys'):
    """
    Computes a DataFrame containing the `Hub` or `Link` key values for any
    satellite row that has been created, updated or deleted by this satellite.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite:       The `Satellite` object that is generating the
                            updated keys.
    :param satellite_owner: A `Hub` or `Link` describing the grain of the updated
                            keys. This must be either `satellite.parent`,or in the
                            case of `Link`s, in `satellite.parent.link_hubs`.
    """

    checkpoint = True
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

    @property
    def name(self) -> str:
        return (
            'keys'
            f'_{self.satellite_owner.full_name}'
            f'_{self.satellite.full_name}'
        )

    @classmethod
    def keys_for_satellite(
            cls,
            runner: SparkRunner,
            satellite: Satellite
    ) -> List[OutputKeys]:
        """
        Generate an `OutputKeys` job for any `Hub` or `Link`, if that Hub
        or Link can have keys generated for it by this satellite or one
        of this satellite's eventual dependencies.

        :param runner:    The `SparkRunner` that is creating these objects.
        :param satellite: The `Satellite` to search for output keys for.
        :return:          A list of `OutputKeys` jobs containing output keys
                          for all relevant `Hub`s and `Link`s.
        """
        return [
            cls(runner, satellite, satellite_owner)
            for satellite_owner in satellite.output_keys()
        ]

    @LazyProperty
    def owner_in_input_keys(self) -> bool:
        """
        :return: Returns True if there are keys that have been updated
                 by this satellite's dependencies for this `SatelliteOwner`.
        """
        return self.satellite_owner in self.satellite.input_keys()

    @LazyProperty
    def owner_in_produced_keys(self) -> bool:
        """
        :return: Returns True if there are keys that have been updated
                 by this satellite for this `SatelliteOwner`.
        """
        return self.satellite_owner in self.satellite.produced_keys()

    @property
    def input_keys_job(self) -> SparkJob:
        """
        :return: The `InputKeys` job for this `Satellite` and `SatelliteOwner`.
        """
        return self.runner.get_job('input_keys', self.satellite, self.satellite_owner)

    @property
    def produced_keys_job(self) -> SparkJob:
        """
        :return: The `ProducedKeys` job for this `Satellite` and `SatelliteOwner`.
        """
        return self.runner.get_job(
            f'produced_{self.satellite_owner.type}_keys',
            self.satellite,
            self.satellite_owner
        )

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
                       {% for alias in job.satellite_owner.link_hubs.keys() %}
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

    @property
    def dependencies(self) -> List[SparkJob]:
        jobs = []
        if self.owner_in_input_keys:
            jobs.append(self.input_keys_job)
        if self.owner_in_produced_keys:
            jobs.append(self.produced_keys_job)
        return jobs
