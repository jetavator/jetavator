from .. import SparkSQLJob
from jetavator.runners.jobs import SerialiseSatellite


class SparkSerialiseSatellite(SparkSQLJob, SerialiseSatellite, register_as='serialise_satellite'):

    sql_template = '''
        INSERT
          INTO {{ job.satellite.table_name }}
        SELECT *
          FROM {{ job.satellite_query_job.name }} AS source
        '''
