import tempfile
import jinja2
import uuid
import sqlalchemy
import datetime
import os
import pandas as pd

import pyspark.sql.functions as F
from functools import reduce
from concurrent.futures import Future
from . import utils
from enum import Enum, auto

COALESCE_PARTITIONS = 10


class SparkJobState(Enum):
    BLOCKED = auto()
    READY = auto()
    RUNNING = auto()
    FINISHED = auto()
    ACKNOWLEDGED = auto()


LIFE_CYCLE_ERROR_STATES = [
    'INTERNAL_ERROR'
]

LIFE_CYCLE_INCOMPLETE_STATES = [
    'PENDING',
    'RUNNING'
]

RESULT_ERROR_STATES = [
    'FAILED',
    'TIMEDOUT',
    'CANCELED'
]


class SparkJob(object):

    def __init__(self, runner, *args, **kwargs):
        self.runner = runner
        self.template_arg_values = kwargs
        self.state_timestamps = {}
        self.set_state(SparkJobState.BLOCKED)
        self.result = None
        for arg, arg_name in zip(args, self.template_args):
            self.template_arg_values[arg_name] = arg
        for arg_name, value in self.template_arg_values.items():
            self.__setattr__(arg_name, value)
        self.name = jinja2.Template(self.name).render(
            self.template_arg_values
        )

    @property
    def logger(self):
        return self.runner.logger

    @classmethod
    def construct_key(cls, *args):
        return '/'.join([
            cls.__name__,
            *[
                '.'.join(vault_object.key)
                for vault_object in args
            ]
        ])

    @property
    def key(self):
        return self.construct_key(*[
            self.template_arg_values[arg]
            for arg in self.key_args
        ])

    @property
    def class_name(self):
        return type(self).__name__

    @property
    def primary_vault_object_key(self):
        return self.template_arg_values[self.key_args[0]].key

    @property
    def name(self):
        raise NotImplementedError

    @property
    def template(self):
        raise NotImplementedError

    @property
    def template_args(self):
        raise NotImplementedError

    @property
    def key_args(self):
        return self.template_args

    @property
    def query(self):
        return jinja2.Template(self.template).render(
            self.template_arg_values
        )

    @property
    def dependencies(self):
        return []

    @property
    def spark(self):
        return self.runner.engine.connection.spark

    def set_state(self, state):
        if state is SparkJobState.RUNNING:
            self.logger.info(f'Starting: {self.name}')
        elif state is SparkJobState.FINISHED:
            self.logger.info(f'Finished: {self.name}')
        self.state = state
        self.state_timestamps[state] = datetime.datetime.now()

    def acknowledge(self):
        self.set_state(SparkJobState.ACKNOWLEDGED)

    def state_timedelta(self, from_state, to_state):
        return (
            self.state_timestamps[to_state]
            - self.state_timestamps[from_state]
        )

    @property
    def wait_time(self):
        return self.state_timedelta(
            SparkJobState.BLOCKED, SparkJobState.READY
        )

    @property
    def queue_time(self):
        return self.state_timedelta(
            SparkJobState.READY, SparkJobState.RUNNING
        )

    @property
    def execution_time(self):
        return self.state_timedelta(
            SparkJobState.RUNNING, SparkJobState.FINISHED
        )

    def execute_query(self):
        try:
            return self.spark.sql(self.query).coalesce(COALESCE_PARTITIONS)
        except Exception as e:
            raise Exception(f'''
                Job completed with error:
                {str(e)}

                Job script:
                {self.query}

                Dependencies:
                {self.dependencies}
            ''')

    def execute(self):
        return self.execute_query()

    @property
    def config(self):
        return self.runner.config

    def require_state(self, state):
        if self.state != state:
            raise Exception(f'Job is not in state {state}.')

    def run(self):
        self.require_state(SparkJobState.READY)
        self.set_state(SparkJobState.RUNNING)
        self.result = self.execute()
        if type(self.result) is not Future:
            self.set_state(SparkJobState.FINISHED)

    def submit_job(self):
        result = self.execute()

    def check_if_blocked(self):
        if self.state == SparkJobState.BLOCKED:
            if self.job_blocked:
                return False
            else:
                self.set_state(SparkJobState.READY)
                return True

    def check_if_finished(self):
        self.require_state(SparkJobState.RUNNING)
        if self.check_if_job_complete():
            self.set_state(SparkJobState.FINISHED)
            return True

    def check_if_job_complete(self):
        if type(self.result) is Future:
            if self.future.exception():
                raise Exception(f'''
                    Job completed with error:
                    {self.future.exception()}

                    Job script:
                    {self.query}
                ''')
            return self.future.done()
        else:
            # Remove this once non-async jobs are all gone!
            self.logger.warn(f'Non-async job {self.name}')
            return True

    @property
    def job_blocked(self):
        return any([
            dependency.state in [
                SparkJobState.BLOCKED,
                SparkJobState.READY,
                SparkJobState.RUNNING
            ]
            for dependency in self.dependencies
        ])


class SparkView(SparkJob):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def checkpoint(self):
        raise NotImplementedError

    @property
    def global_view(self):
        raise NotImplementedError

    def execute(self):
        df = super().execute()

        if self.checkpoint:
            df = df.localCheckpoint()

        if self.global_view:
            df = df.createOrReplaceGlobalTempView(self.name)
        else:
            df = df.createOrReplaceTempView(self.name)

        return df


class CreateSource(SparkJob):
    name = 'create_source_{{source.name}}'
    template = '''
        {{source_ddl}}
        {% if csv_path is defined %}
        USING csv
        OPTIONS (
            path "{{csv_path}}",
            header "true",
            inferSchema "false"
        )
        {% endif %}
        '''
    template_args = ['source', 'source_ddl', 'csv_path']
    key_args = ['source']

    def __init__(self, runner, source):
        source_ddl = runner.engine.connection.compile_sqlalchemy(
            source.sql_model
            .create_table(source.sql_model.table)[0]
        )
        if runner.engine.connection.source_csv_exists(source):
            super().__init__(
                runner,
                source,
                source_ddl=source_ddl.replace(
                    'CREATE TABLE', 'CREATE TEMPORARY TABLE'),
                csv_path=runner.engine.connection.csv_file_path(source)
            )
        else:
            super().__init__(
                runner,
                source,
                source_ddl=source_ddl.replace(
                    'CREATE TABLE', 'CREATE TABLE IF NOT EXISTS'),
            )
            self.logger.warn(f'CSV file not found for source: {source}')


class DropSource(SparkJob):
    name = 'drop_source_{{source.name}}'
    template = '''
        DROP TABLE source_{{source.name}}
        '''
    template_args = ['source']
    key_args = ['source']

    @property
    def dependencies(self):
        return [
            self.runner.get_job(SatelliteQuery, satellite)
            for satellite in self.source.dependent_satellites
        ]


class InputKeys(SparkView):
    name = (
        'vault_updates'
        '_{{satellite_owner.full_name}}'
        '_{{satellite.full_name}}'
    )
    template_args = ['satellite', 'satellite_owner', 'dependent_satellites']
    key_args = ['satellite', 'satellite_owner']
    checkpoint = False
    global_view = False

    def execute_query(self):

        keys = [self.satellite_owner.key_column_name]
        if self.satellite_owner.type == 'link':
            keys += [
                f'hub_{alias}_key'
                for alias in self.satellite_owner.link_hubs.keys()
            ]

        key_tables = [
            self.spark.table(
                f'keys_{self.satellite_owner.full_name}'
                f'_{dependent_satellite.full_name}'
            )
            for dependent_satellite in self.dependent_satellites
        ]

        def combine_key_tables(left, right):
            return (
                left
                .join(
                    right,
                    left[keys[0]] == right[keys[0]],
                    how='full')
                .select(
                    *[
                        F.coalesce(left[key], right[key]).alias(key)
                        for key in keys
                    ],
                    F.concat(
                        F.coalesce(left.key_source, F.array()),
                        F.coalesce(right.key_source, F.array())
                    ).alias('key_source')))

        return reduce(combine_key_tables, key_tables)

    def __init__(self, runner, satellite, satellite_owner):
        super().__init__(
            runner,
            satellite,
            satellite_owner,
            dependent_satellites=satellite.dependent_satellites_by_owner(
                satellite_owner.key)
        )

    @property
    def dependencies(self):
        return [
            self.runner.satellite_owner_output_keys(
                dependent_satellite,
                self.satellite_owner,
                self.satellite_owner.type
            )
            for dependent_satellite in self.dependent_satellites
        ]


class SatelliteQuery(SparkView):
    name = 'vault_updates_{{satellite.full_name}}'
    template = '{{sql}}'
    template_args = ['satellite', 'sql']
    key_args = ['satellite']
    checkpoint = True
    global_view = False

    def __init__(self, runner, satellite):
        super().__init__(
            runner,
            satellite,
            sql=runner.engine.connection.compile_sqlalchemy(
                satellite.pipeline.sql_model.pipeline_query())
        )

    @property
    def dependencies(self):
        return [
            *self.runner.input_keys(self.satellite, 'hub'),
            *self.runner.input_keys(self.satellite, 'link'),
            *[
                self.runner.get_job(
                    SerialiseSatellite, dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'satellite'
                and dep.view in ['current', 'history']
            ],
            *[
                self.runner.get_job(
                    CreateSource, dep.object_reference)
                for dep in self.satellite.pipeline.dependencies
                if dep.type == 'source'
            ]
        ]


class ProducedHubKeys(SparkView):
    name = 'produced_keys_{{hub.full_name}}_{{satellite.full_name}}'
    template = '''
        {% if key_columns|length > 1 %}
        SELECT {{ hub.key_column_name }},
               collect_set(key_source) AS key_source
          FROM (
                {% for column in key_columns %}
                SELECT {{ column.name }} AS {{ hub.key_column_name }},
                       '{{ column.source }}' AS key_source
                  FROM vault_updates_{{satellite.full_name}}

                {{ "UNION ALL" if not loop.last }}
                {% endfor %}
               ) AS keys
         GROUP
            BY {{ hub.key_column_name }}
        {% else %}
        SELECT {{ key_columns[0].name }} AS {{ hub.key_column_name }},
               array('{{ key_columns[0].source }}') AS key_source
          FROM vault_updates_{{satellite.full_name}}
         GROUP
            BY {{ key_columns[0].name }}
        {% endif %}
        '''
    template_args = ['satellite', 'hub', 'key_columns']
    key_args = ['satellite', 'hub']
    checkpoint = True
    global_view = False

    def __init__(self, runner, satellite, hub):
        super().__init__(
            runner,
            satellite=satellite,
            hub=hub,
            key_columns=satellite.hub_key_columns[hub.name]
        )

    @property
    def dependencies(self):
        return [self.runner.get_job(SatelliteQuery, self.satellite)]


class ProducedLinkKeys(SparkView):
    name = 'produced_keys_{{link.full_name}}_{{satellite.full_name}}'
    template = '''
        SELECT {{ link.key_column_name }}
               , array('sat_{{ satellite.name }}') AS key_source
               {% for alias in link.link_hubs.keys() %}
               , hub_{{alias}}_key
               {% endfor %}
          FROM vault_updates_{{satellite.full_name}}
        '''
    template_args = ['satellite', 'link']
    checkpoint = False
    global_view = False

    @property
    def dependencies(self):
        return [self.runner.get_job(SatelliteQuery, self.satellite)]


class OutputKeysFromSatellite(SparkView):
    name = 'keys_{{satellite_owner.full_name}}_{{satellite.full_name}}'
    template = '''
        SELECT * FROM produced_keys_{{satellite_owner.full_name}}_{{satellite.full_name}}
        '''
    template_args = ['satellite', 'satellite_owner']
    checkpoint = False
    global_view = False

    @property
    def dependencies(self):
        if self.satellite_owner.type == "hub":
            produced_keys_class = ProducedHubKeys
        else:
            produced_keys_class = ProducedLinkKeys
        return [
            self.runner.get_job(
                produced_keys_class, self.satellite, self.satellite_owner)
        ]


class OutputKeysFromDependencies(SparkView):
    name = 'keys_{{satellite_owner.full_name}}_{{satellite.full_name}}'
    template = '''
        SELECT * FROM
        vault_updates_{{satellite_owner.full_name}}_{{satellite.full_name}}
        '''
    template_args = ['satellite', 'satellite_owner']
    checkpoint = False
    global_view = False

    @property
    def dependencies(self):
        return [self.runner.get_job(
            InputKeys, self.satellite, self.satellite_owner)]


class OutputKeysFromBoth(SparkView):
    name = 'keys_{{satellite_owner.full_name}}_{{satellite.full_name}}'
    template = '''
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
    template_args = ['satellite', 'satellite_owner']
    checkpoint = True
    global_view = False

    @property
    def dependencies(self):
        if self.satellite_owner.type == "hub":
            produced_keys_class = ProducedHubKeys
        else:
            produced_keys_class = ProducedLinkKeys
        return [
            self.runner.get_job(
                produced_keys_class, self.satellite, self.satellite_owner),
            self.runner.get_job(
                InputKeys, self.satellite, self.satellite_owner)
        ]


class StarKeys(SparkView):
    name = 'keys_{{satellite_owner.full_name}}'
    template = '''
        SELECT {{ satellite_owner.key_column_name }},
               {% if satellite_owner.type == "link" %}
               {% for alias in satellite_owner.link_hubs.keys() %}
               first(hub_{{alias}}_key) AS hub_{{alias}}_key,
               {% endfor %}
               {% endif %}
               flatten(collect_set(key_source)) AS key_source
          FROM (
                {% for satellite in satellite_owner.satellites_containing_keys.values() %}
                SELECT
                       {{ satellite_owner.key_column_name }},
                       {% if satellite_owner.type == "link" %}
                       {% for alias in satellite_owner.link_hubs.keys() %}
                       hub_{{alias}}_key,
                       {% endfor %}
                       {% endif %}
                       key_source
                  FROM keys_{{satellite_owner.full_name}}_{{satellite.full_name}}

                {{ "UNION ALL" if not loop.last }}
                {% endfor %}
               ) AS keys
         GROUP
            BY {{ satellite_owner.key_column_name }}
        '''
    template_args = ['satellite_owner']
    checkpoint = True
    global_view = False

    @property
    def dependencies(self):
        return [
            self.runner.satellite_owner_output_keys(
                satellite,
                self.satellite_owner,
                self.satellite_owner.type
            )
            for satellite
            in self.satellite_owner.satellites_containing_keys.values()
        ]


class StarData(SparkView):
    name = 'updates_{{satellite_owner.sql_model.star_table_name}}'
    template = '''
        SELECT keys.{{satellite_owner.key_column_name}}

               {% if satellite_owner.type == "link" %}
               {% for alias in satellite_owner.link_hubs.keys() %}
               , keys.hub_{{alias}}_key
               {% endfor %}
               {% endif %}

               , keys.key_source

               {% for satellite in satellite_owner.star_satellites.values() %}
               , array_contains(
                    keys.key_source,
                    '{{satellite.full_name}}'
                 ) AS update_ind_{{satellite.name}}
               {% endfor %}

               {% if satellite_owner.star_satellites.values() | length > 1 %}
               , (LEAST(
                   {% for satellite in satellite_owner.star_satellites.values() %}
                   {{satellite.name}}.sat_deleted_ind{{"," if not loop.last}}
                   {% endfor %}
               ) == 1) AS deleted_ind
               {% elif satellite_owner.star_satellites.values() | length == 1 %}
               {% for satellite in satellite_owner.star_satellites.values() %}
               , ({{satellite.name}}.sat_deleted_ind == 1) AS deleted_ind
               {% endfor %}
               {% else %}
               , false AS deleted_ind
               {% endif %}

               {% for satellite in satellite_owner.star_satellites.values() %}
               {% for column in satellite.columns.keys() %}
               , {{satellite.name}}.{{column}}
               {% endfor %}
               {% endfor %}

          FROM keys_{{satellite_owner.full_name}} AS keys
          {% for satellite in satellite_owner.star_satellites.values() %}
          LEFT
          JOIN vault_updates_{{satellite.full_name}} AS {{satellite.name}}
            ON {{satellite.name}}.{{satellite.parent.key_column_name}} =
               keys.{{satellite.parent.key_column_name}}
          {% endfor %}
        '''
    template_args = ['satellite_owner']
    checkpoint = True
    global_view = False

    @property
    def dependencies(self):
        return [
            self.runner.get_job(StarKeys, self.satellite_owner),
            *[
                self.runner.get_job(SatelliteQuery, satellite)
                for satellite in self.satellite_owner.star_satellites.values()
            ]
        ]


class StarMerge(SparkJob):
    name = 'merge_{{satellite_owner.sql_model.star_table_name}}'
    template_args = ['satellite_owner']

    def execute_query(self):
        path = [
            row
            for row in self.spark.sql(
                'DESCRIBE FORMATTED ' +
                self.satellite_owner.sql_model.star_table_name
            ).collect()
            if row['col_name'] == 'Location'
        ][0]['data_type']

        source = self.spark.table(
            'updates_' +
            self.satellite_owner.sql_model.star_table_name
        )

        # Import has to happen inline because delta library is installed
        # at runtime by PySpark. Not ideal as not PEP8 compliant!
        # Create a setuptools-compatible mirror repo instead?
        from delta.tables import DeltaTable

        (
            DeltaTable
            .forPath(self.spark, path)
            .alias('target').merge(
                source.alias('source'),
                (
                    f'target.{self.satellite_owner.key_column_name}'
                    f' = source.{self.satellite_owner.key_column_name}'
                )
            )
            .whenMatchedDelete(condition='source.deleted_ind = 1')
            .whenMatchedUpdate(set={
                column: f"""
                    CASE WHEN array_contains(
                            source.key_source,
                            '{satellite.full_name}'
                         )
                         THEN source.{column}
                         ELSE target.{column}
                         END
                    """
                for satellite in self.satellite_owner.star_satellites.values()
                for column in satellite.columns.keys()
            })
            .whenNotMatchedInsertAll()
            .execute()
        )

    @property
    def dependencies(self):
        return [self.runner.get_job(StarData, self.satellite_owner)]


class SerialiseSatellite(SparkJob):
    name = 'serialise_sat_{{satellite.name}}'
    template = '''
        INSERT
          INTO {{satellite.sql_model.table.name}}
        SELECT *
          FROM vault_updates_{{satellite.full_name}} AS source
        '''
    template_args = ['satellite']

    @property
    def dependencies(self):
        return [self.runner.get_job(SatelliteQuery, self.satellite)]


class SparkRunner(object):

    def __init__(self, engine, project):
        self.engine = engine
        self.project = project
        self.jobs = {}
        self.create_jobs()

    @property
    def logger(self):
        return self.engine.logger

    @property
    def config(self):
        return self.engine.config

    @property
    def connection(self):
        return self.engine.connection

    def get_job(self, job_class, *args):
        return self.jobs[job_class.construct_key(*args)]

    def get_or_create_job(self, job_class, *args):
        key = job_class.construct_key(*args)
        if key not in self.jobs:
            self.jobs[key] = job_class(self, *args)
        return self.jobs[key]

    def create_jobs(self):
        for source in self.project.sources.values():
            self.source_jobs(source)
        for satellite in self.project.satellites.values():
            self.satellite_jobs(satellite)
        for satellite_owner in self.project.satellite_owners.values():
            if satellite_owner.satellites_containing_keys:
                self.star_jobs(satellite_owner)

    def jobs_in_state(self, states):
        return {
            name: job
            for name, job in self.jobs.items()
            if job.state in states
        }

    def source_jobs(self, source):
        jobs = [
            self.get_or_create_job(CreateSource, source)
        ]
        if self.engine.connection.source_csv_exists(source):
            jobs += [
                self.get_or_create_job(DropSource, source)
            ]
        return jobs

    def satellite_jobs(self, satellite):
        return [
            *self.input_keys(satellite, 'hub'),
            *self.input_keys(satellite, 'link'),
            self.get_or_create_job(SatelliteQuery, satellite),
            *self.produced_hub_keys(satellite),
            *self.produced_link_keys(satellite),
            *self.output_keys(satellite, 'hub'),
            *self.output_keys(satellite, 'link'),
            self.get_or_create_job(SerialiseSatellite, satellite)
        ]

    # TODO: Eliminate StarMerge if star datastore is external to Spark
    def star_jobs(self, satellite_owner):
        return [
            self.get_or_create_job(StarKeys, satellite_owner),
            self.get_or_create_job(StarData, satellite_owner),
            self.get_or_create_job(StarMerge, satellite_owner)
        ]

    def input_keys(self, satellite, type):
        return [
            self.get_or_create_job(InputKeys, satellite, satellite_owner)
            for satellite_owner in satellite.input_keys(type).values()
        ]

    def satellite_owner_output_keys(self, satellite, satellite_owner, type):
        if satellite_owner.name not in satellite.input_keys(type):
            job_class = OutputKeysFromSatellite
        elif satellite_owner.name not in satellite.produced_keys(type):
            job_class = OutputKeysFromDependencies
        else:
            job_class = OutputKeysFromBoth

        return self.get_or_create_job(job_class, satellite, satellite_owner)

    def output_keys(self, satellite, type):
        return [
            self.satellite_owner_output_keys(satellite, satellite_owner, type)
            for satellite_owner in satellite.output_keys(type).values()
        ]

    def produced_hub_keys(self, satellite):
        return [
            self.get_or_create_job(
                ProducedHubKeys,
                satellite,
                hub
            )
            for hub in satellite.produced_keys('hub').values()
        ]

    def produced_link_keys(self, satellite):
        return [
            self.get_or_create_job(
                ProducedLinkKeys,
                satellite,
                link
            )
            for link in satellite.produced_keys('link').values()
        ]

    @property
    def blocked_jobs(self):
        return self.jobs_in_state([SparkJobState.BLOCKED])

    @property
    def ready_jobs(self):
        return self.jobs_in_state([SparkJobState.READY])

    @property
    def running_jobs(self):
        return self.jobs_in_state([SparkJobState.RUNNING])

    @property
    def finished_jobs(self):
        return self.jobs_in_state([SparkJobState.FINISHED])

    @property
    def acknowledged_jobs(self):
        return self.jobs_in_state([SparkJobState.ACKNOWLEDGED])

    def run(self):
        self.start_ready_jobs()
        if not self.running_jobs and not self.finished_jobs:
            raise Exception("Dependency error. No jobs could be started.")
        while self.running_jobs or self.finished_jobs:
            self.check_for_finished_jobs()
            if self.finished_jobs:
                self.acknowledge_finished_jobs()
                self.start_ready_jobs()

    def check_for_finished_jobs(self):
        for job in self.running_jobs.values():
            job.check_if_finished()

    def acknowledge_finished_jobs(self):
        for job in self.finished_jobs.values():
            job.acknowledge()

    def start_ready_jobs(self):
        self.find_ready_jobs()
        for job in self.ready_jobs.values():
            job.run()

    def find_ready_jobs(self):
        for job in self.blocked_jobs.values():
            job.check_if_blocked()

    def performance_data(self):
        return pd.DataFrame([
            (
                key,
                '.'.join(job.primary_vault_object_key),
                job.class_name,
                job.queue_time,
                job.wait_time,
                job.execution_time.total_seconds()
            )
            for key, job in self.jobs.items()
        ], columns=[
            'key',
            'primary_vault_object_key',
            'class_name',
            'queue_time',
            'wait_time',
            'execution_time'
        ])
