from typing import List

from jetavator import KeyType
from jetavator.schema_registry import Satellite

from .. import SparkJobABC, SparkSQLView, SparkRunnerABC


class SatelliteQuery(SparkSQLView, register_as='satellite_query'):
    name_template = 'vault_updates_{{satellite.full_name}}'
    sql_template = '{{sql}}'
    template_args = ['satellite', 'sql']
    key_args = ['satellite']
    checkpoint = True
    global_view = False

    def __init__(
        self,
        runner: SparkRunnerABC,
        satellite: Satellite
    ) -> None:
        super().__init__(
            runner,
            satellite,
            sql=runner.compute_service.compile_sqlalchemy(
                satellite.pipeline.sql_model.pipeline_query())
        )
        self.satellite = satellite

    @property
    def dependencies(self) -> List[SparkJobABC]:
        return [
            *self.runner.input_keys(self.satellite, KeyType.HUB),
            *self.runner.input_keys(self.satellite, KeyType.LINK),
            *[
                self.runner.get_job('serialise_satellite', dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'satellite'
                and dep.view in ['current', 'history']
            ],
            *[
                self.runner.get_job('create_source', dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'source'
            ]
        ]
