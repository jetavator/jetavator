from typing import List

from jetavator.schema_registry import Satellite

from .. import SparkSQLView, SparkRunner, SparkJob


class SatelliteQuery(SparkSQLView, register_as='satellite_query'):
    """
    Computes a DataFrame containing the result of the user query defined in
    `Satellite.pipeline` for a particular `Satellite`.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite:       The `Satellite` object containing the query definition.
    """

    sql_template = '{{job.sql}}'
    checkpoint = True
    global_view = False

    def __init__(
            self,
            runner: SparkRunner,
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
    def dependencies(self) -> List[SparkJob]:
        return [
            *[
                self.runner.get_job('input_keys', self.satellite, satellite_owner)
                for satellite_owner in self.satellite.input_keys()
            ],
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
