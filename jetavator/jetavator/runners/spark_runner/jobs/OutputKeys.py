from abc import ABC
from typing import List

from jetavator.schema_registry import Satellite, SatelliteOwner

from .. import SparkSQLView, SparkJobABC, SparkRunnerABC


class OutputKeys(SparkSQLView, ABC):
    template_args = ['satellite', 'satellite_owner']
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


class OutputKeysFromDependencies(
    OutputKeys,
    register_as='output_keys_from_dependencies'
):
    name_template = 'keys_{{satellite_owner.full_name}}_{{satellite.full_name}}'
    sql_template = '''
        SELECT * FROM
        vault_updates_{{satellite_owner.full_name}}_{{satellite.full_name}}
        '''

    @property
    def dependencies(self) -> List[SparkJobABC]:
        return [
            self.runner.get_job('input_keys', self.satellite, self.satellite_owner)
        ]


class OutputKeysFromSatellite(
    OutputKeys,
    register_as='output_keys_from_satellite'
):
    name_template = 'keys_{{satellite_owner.full_name}}_{{satellite.full_name}}'
    sql_template = '''
        SELECT * FROM produced_keys_{{satellite_owner.full_name}}_{{satellite.full_name}}
        '''

    @property
    def dependencies(self) -> List[SparkJobABC]:
        if self.satellite_owner.type == "hub":
            produced_keys_class = 'produced_hub_keys'
        else:
            produced_keys_class = 'produced_link_keys'
        return [
            self.runner.get_job(produced_keys_class, self.satellite, self.satellite_owner)
        ]


class OutputKeysFromBoth(
    OutputKeys,
    register_as='output_keys_from_both'
):
    name_template = 'keys_{{satellite_owner.full_name}}_{{satellite.full_name}}'
    sql_template = '''
        SELECT COALESCE(input_keys.{{satellite_owner.key_column_name}},
                        produced_keys.{{satellite_owner.key_column_name}})
                     AS {{satellite_owner.key_column_name}},
               {% if satellite_owner.type == "link" %}
               {% for alias in satellite_owner.link_hubs.keys() %}
               COALESCE(input_keys.hub_{{alias}}_key,
                        produced_keys.hub_{{alias}}_key)
                     AS hub_{{alias}}_key,
               {% endfor %}
               {% endif %}
               CONCAT(COALESCE(input_keys.key_source, array()),
                      COALESCE(produced_keys.key_source, array()))
                   AS key_source
          FROM vault_updates_{{satellite_owner.full_name}}_{{satellite.full_name}} AS input_keys
               FULL JOIN produced_keys_{{satellite_owner.full_name}}_{{satellite.full_name}}
                      AS produced_keys
                      ON input_keys.{{satellite_owner.key_column_name}}
                       = produced_keys.{{satellite_owner.key_column_name}}

        '''

    @property
    def dependencies(self) -> List[SparkJobABC]:
        return [
            *OutputKeysFromDependencies(
                self.runner, self.satellite, self.satellite_owner
            ).dependencies,
            *OutputKeysFromSatellite(
                self.runner, self.satellite, self.satellite_owner
            ).dependencies,
        ]
