from typing import List

from jetavator.schema_registry import Satellite

from .. import SparkSQLView, SparkRunnerABC


class SatelliteQuery(SparkSQLView, register_as='satellite_query'):
    sql_template = '{{job.sql}}'
    checkpoint = True
    global_view = False

    def __init__(
        self,
        runner: SparkRunnerABC,
        satellite: Satellite
    ) -> None:
        super().__init__(
            runner,
            satellite
        )
        self.satellite = satellite

    @property
    def name(self) -> str:
        return f'vault_updates_{self.satellite.full_name}'

    @property
    def sql(self) -> str:
        return self.runner.compute_service.compile_sqlalchemy(
            self.satellite.pipeline.sql_model.pipeline_query())

    @property
    def dependency_keys(self) -> List[str]:
        return [
            *[
                self.construct_job_key('input_keys', self.satellite, satellite_owner)
                for satellite_owner in self.satellite.input_keys()
            ],
            *[
                self.construct_job_key('serialise_satellite', dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'satellite'
                and dep.view in ['current', 'history']
            ],
            *[
                self.construct_job_key('create_source', dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'source'
            ]
        ]
