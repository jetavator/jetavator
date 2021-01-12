from .. import SparkSQLJob
from jetavator.runners.jobs import StarMerge


class SparkStarMerge(SparkSQLJob, StarMerge, register_as='star_merge'):

    sql_template = '''
        MERGE 
         INTO {{ job.satellite_owner.star_table_name }} AS target
        USING {{ job.star_data_job.name }} AS source
           ON target.{{ job.satellite_owner.key_column_name }}
            = source.{{ job.satellite_owner.key_column_name }}
         WHEN MATCHED AND source.deleted_ind = 1 THEN DELETE
         {% for column, satellite in job.star_column_references %}
         {{ "WHEN MATCHED THEN UPDATE SET" if loop.first }}
             {{ column }} = CASE WHEN array_contains(
                 source.key_source,
                 '{{ satellite.full_name }}'
             )
             THEN source.{{ column }}
             ELSE target.{{ column }}
             END
           {{ "," if not loop.last }}
         {% endfor %}
         WHEN NOT MATCHED THEN INSERT *
        '''
