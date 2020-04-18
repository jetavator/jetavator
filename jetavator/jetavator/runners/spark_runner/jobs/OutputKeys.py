from __future__ import annotations

from abc import ABC
from typing import List

from jetavator import KeyType
from jetavator.schema_registry import Satellite, SatelliteOwner

from .. import SparkSQLView, SparkRunnerABC


class OutputKeys(SparkSQLView, ABC):
    checkpoint = True
    global_view = False

    def __init__(
            self,
            runner: SparkRunnerABC,
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
        runner: SparkRunnerABC,
        satellite: Satellite,
        key_type: KeyType
    ) -> List[OutputKeys]:
        # TODO: Implement KeyType throughout schema_registry to avoid .value conversions
        if type(key_type) is str:
            key_type = KeyType.HUB if key_type == 'hub' else KeyType.LINK
        return [
            cls.satellite_owner_output_keys(
                runner, satellite, satellite_owner, key_type)
            for satellite_owner in satellite.output_keys(key_type.value).values()
        ]

    @classmethod
    def satellite_owner_output_keys(
        cls,
        runner: SparkRunnerABC,
        satellite: Satellite,
        satellite_owner: SatelliteOwner,
        key_type: KeyType
    ) -> OutputKeys:
        # TODO: Implement KeyType throughout schema_registry to avoid .value conversions
        if type(key_type) is str:
            key_type = KeyType.HUB if key_type == 'hub' else KeyType.LINK
        if satellite_owner.name not in satellite.input_keys(key_type.value):
            job_class = 'output_keys_from_satellite'
        elif satellite_owner.name not in satellite.produced_keys(key_type.value):
            job_class = 'output_keys_from_dependencies'
        else:
            job_class = 'output_keys_from_both'
        return cls.registered_subclass_instance(
            job_class, runner, satellite, satellite_owner)


class OutputKeysFromDependencies(
    OutputKeys,
    register_as='output_keys_from_dependencies'
):
    sql_template = '''
        SELECT * FROM
        vault_updates_{{job.satellite_owner.full_name}}_{{job.satellite.full_name}}
        '''

    @property
    def dependency_keys(self) -> List[str]:
        return [
            self.construct_job_key('input_keys', self.satellite, self.satellite_owner)
        ]


class OutputKeysFromSatellite(
    OutputKeys,
    register_as='output_keys_from_satellite'
):
    sql_template = '''
        SELECT * FROM produced_keys_{{job.satellite_owner.full_name}}_{{job.satellite.full_name}}
        '''

    @property
    def dependency_keys(self) -> List[str]:
        if self.satellite_owner.type == "hub":
            produced_keys_class = 'produced_hub_keys'
        else:
            produced_keys_class = 'produced_link_keys'
        return [
            self.construct_job_key(produced_keys_class, self.satellite, self.satellite_owner)
        ]


class OutputKeysFromBoth(
    OutputKeys,
    register_as='output_keys_from_both'
):
    sql_template = '''
        SELECT COALESCE(input_keys.{{job.satellite_owner.key_column_name}},
                        produced_keys.{{job.satellite_owner.key_column_name}})
                     AS {{job.satellite_owner.key_column_name}},
               {% if job.satellite_owner.type == "link" %}
               {% for alias in job.satellite_owner.link_hubs.keys() %}
               COALESCE(input_keys.hub_{{alias}}_key,
                        produced_keys.hub_{{alias}}_key)
                     AS hub_{{alias}}_key,
               {% endfor %}
               {% endif %}
               CONCAT(COALESCE(input_keys.key_source, array()),
                      COALESCE(produced_keys.key_source, array()))
                   AS key_source
          FROM vault_updates_{{job.satellite_owner.full_name}}_{{job.satellite.full_name}} AS input_keys
               FULL JOIN produced_keys_{{job.satellite_owner.full_name}}_{{job.satellite.full_name}}
                      AS produced_keys
                      ON input_keys.{{job.satellite_owner.key_column_name}}
                       = produced_keys.{{job.satellite_owner.key_column_name}}

        '''

    @property
    def dependency_keys(self) -> List[str]:
        return [
            *OutputKeysFromDependencies(
                self.runner, self.satellite, self.satellite_owner
            ).dependency_keys,
            *OutputKeysFromSatellite(
                self.runner, self.satellite, self.satellite_owner
            ).dependency_keys,
        ]
