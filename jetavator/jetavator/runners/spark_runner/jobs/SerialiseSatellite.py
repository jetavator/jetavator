from typing import List

from jetavator.schema_registry import Satellite

from .. import SparkSQLJob, SparkRunner, SparkJob


class SerialiseSatellite(SparkSQLJob, register_as='serialise_satellite'):
    """
    Serialises the created, updated or deleted rows for a `Satellite` to its
    Delta Lake table.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite:       The `Satellite` object containing the query definition.
    """

    sql_template = '''
        INSERT
          INTO {{ job.satellite.table_name }}
        SELECT *
          FROM {{ job.satellite_query_job.name }} AS source
        '''

    def __init__(self, runner: SparkRunner, satellite: Satellite) -> None:
        super().__init__(runner, satellite)
        self.satellite = satellite

    @property
    def name(self) -> str:
        return f'serialise_sat_{self.satellite.name}'

    @property
    def satellite_query_job(self) -> SparkJob:
        """
        :return: The `SatelliteQuery` job that contains the updated data.
        """
        return self.runner.get_job('satellite_query', self.satellite)

    @property
    def dependencies(self) -> List[SparkJob]:
        return [self.satellite_query_job]
