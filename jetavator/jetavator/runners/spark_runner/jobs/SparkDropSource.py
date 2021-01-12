from .. import SparkSQLJob
from jetavator.runners.jobs import DropSource


class SparkDropSource(SparkSQLJob, DropSource, register_as='drop_source'):

    sql_template = '''
        DROP TABLE source_{{job.source.name}}
        '''
