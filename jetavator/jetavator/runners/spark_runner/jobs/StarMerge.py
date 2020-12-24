from typing import List, Tuple, Iterator

from jetavator.schema_registry import SatelliteOwner, Satellite

from .. import SparkJob, SparkSQLJob, SparkRunner


class StarMerge(SparkSQLJob, register_as='star_merge'):
    """
    Merges the created, updated or deleted rows into the the star schema
    Delta Lake table for this particular `Hub` or `Link`.

    :param runner:          The `SparkRunner` that created this object.
    :param satellite_owner: The `Hub` or `Link` object that is being used to create
                            a Dimension or Fact table, respectively.
    """

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

    def __init__(
            self,
            runner: SparkRunner,
            satellite_owner: SatelliteOwner
    ) -> None:
        super().__init__(runner, satellite_owner)
        self.satellite_owner = satellite_owner

    @property
    def name(self) -> str:
        return f'merge_{self.satellite_owner.star_table_name}'

    @property
    def star_data_job(self) -> SparkJob:
        """
        :return: The `StarData` job that contains the updated keys and data.
        """
        return self.runner.get_job('star_data', self.satellite_owner)

    @property
    def star_column_references(self) -> Iterator[Tuple[str, Satellite]]:
        """
        :return: An iterator of tuples containing column names in the star schema
                 and their owning satellites.
        """
        for satellite in self.satellite_owner.star_satellites.values():
            for column in satellite.columns.keys():
                yield column, satellite

    @property
    def dependencies(self) -> List[SparkJob]:
        return [self.star_data_job]
