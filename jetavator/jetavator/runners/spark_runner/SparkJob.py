from __future__ import annotations

import datetime
import jinja2

from logging import Logger
from typing import List

from abc import ABC, abstractmethod

from concurrent.futures import Future

from lazy_property import LazyProperty
from pyspark.sql import DataFrame, SparkSession

from wysdom.mixins import RegistersSubclasses
from jetavator.schema_registry import VaultObject

from .SparkJobState import SparkJobState
from .. import Runner

COALESCE_PARTITIONS = 10


class SparkJob(RegistersSubclasses, ABC):
    """
    Base class for all Spark jobs.

    :param runner: A `Runner` which must implement the abstract interface
                   `SparkRunnerABC`. Used for providing execution services
                   and allowing SparkJobs to look up other SparkJobs for
                   dependency purposes.

    :param *args:  A variable-length list of `VaultObject` instances associated
                   with the SparkJob. The number and types of these objects
                   depends on the subclass implementation. The combination of
                   VaultObjects supplied, together with the registered name of
                   the SparkJob subclass, must constitute a unique key for that
                   SparkJob instance.
    """

    @abstractmethod
    def __init__(self, runner: Runner, *args: VaultObject) -> None:
        """
        Abstract class constructor to be extended and called by subclasses.
        """
        super().__init__()
        self.runner = runner
        self.state_timestamps = {}
        self.result = None
        self._vault_objects = args
        self._state = None
        self._set_state(SparkJobState.BLOCKED)

    def __repr__(self) -> str:
        vault_object_reprs = ', '.join([repr(obj) for obj in self._vault_objects])
        return f'{type(self).__name__}({vault_object_reprs})'

    @property
    def logger(self) -> Logger:
        """
        Python `Logger` instance for raising log messages.
        """
        return self.runner.logger

    @LazyProperty
    def key(self) -> str:
        """
        The unique key for this SparkJob, based on its class and arguments.
        """
        return self.spark_job_key(
            self.registered_name,
            *self._vault_objects
        )

    @property
    def primary_vault_object(self) -> VaultObject:
        """
        The first VaultObject argument for this SparkJob. Used for reporting.
        """
        return self._vault_objects[0]

    @staticmethod
    def spark_job_key(
            registered_name: str,
            *args: VaultObject
    ) -> str:
        return '/'.join([
            registered_name,
            *[
                '.'.join(vault_object.key)
                for vault_object in args
            ]
        ])

    @property
    @abstractmethod
    def name(self) -> str:
        """
        The name of this SparkJob instance for logging purposes. If the
        subclass is also a SparkView, this also becomes the view name.
        """
        pass

    @property
    def dependencies(self) -> List[SparkJobABC]:
        """
        A list of SparkJobs that must be executed before this SparkJob
        can start.
        """
        return []

    @property
    def spark(self) -> SparkSession:
        """
        The Spark session that this SparkJob will use for execution.
        """
        return self.runner.compute_service.spark

    @property
    def state(self) -> SparkJobState:
        """
        The current execution state of the SparkJob.
        """
        return self._state

    @property
    def wait_time(self) -> datetime.timedelta:
        """
        The time this job spent being blocked by other jobs before
        being ready to execute. Requires a minimum state of
        `SparkJobState.READY`.
        """
        return self._state_timedelta(
            SparkJobState.BLOCKED, SparkJobState.READY
        )

    @property
    def queue_time(self) -> datetime.timedelta:
        """
        The time this job spent between being ready to execute and starting
         to execute. Requires a minimum state of `SparkJobState.RUNNING`.
        """
        return self._state_timedelta(
            SparkJobState.READY, SparkJobState.RUNNING
        )

    @property
    def execution_time(self) -> datetime.timedelta:
        """
        The time this job spent being executed. Requires a minimum state of
        `SparkJobState.FINISHED`.
        """
        return self._state_timedelta(
            SparkJobState.RUNNING, SparkJobState.FINISHED
        )

    @abstractmethod
    def execute(self) -> DataFrame:
        """
        Execute the underlying Spark job and return the resultant `DataFrame`.
        """
        pass

    def check_if_ready(self) -> bool:
        """
        Check if a blocked job is now ready by examining the state of its
        dependencies. If it is, update its state to `SparkJobState.READY`
        and return True.
        """
        if self.state == SparkJobState.BLOCKED:
            if self._has_blocking_dependencies:
                return False
            else:
                self._set_state(SparkJobState.READY)
                return True

    def run(self) -> None:
        """
        Start executing a ready job and update its state to
        `SparkJobState.RUNNING`.
        """
        self._require_state(SparkJobState.READY)
        self._set_state(SparkJobState.RUNNING)
        self.result = self.execute()
        if type(self.result) is not Future:
            self._set_state(SparkJobState.FINISHED)

    def check_if_finished(self) -> bool:
        """
        Check if a running job is now finished by examining its execution
        state. If it is, update its state to `SparkJobState.FINISHED`
        and return True.
        """
        self._require_state(SparkJobState.RUNNING)
        if self._check_if_execution_complete():
            self._set_state(SparkJobState.FINISHED)
            return True

    def acknowledge(self) -> None:
        """
        Acknowledge a completed job. Does nothing except to remove it
        from the list of recently finished jobs
        (see `SparkRunner.finished_jobs`)
        """
        self._require_state(SparkJobState.FINISHED)
        self._set_state(SparkJobState.ACKNOWLEDGED)

    def _check_if_execution_complete(self) -> bool:
        if type(self.result) is Future:
            if self.result.exception():
                raise RuntimeError(self.result.exception())
            return self.result.done()
        else:
            # TODO: Remove this once non-async jobs are all gone! See #9
            self.logger.warning(f'Non-async job {self.name}')
            return True

    @property
    def _has_blocking_dependencies(self) -> bool:
        return any([
            dependency.state in [
                SparkJobState.BLOCKED,
                SparkJobState.READY,
                SparkJobState.RUNNING
            ]
            for dependency in self.dependencies
        ])

    def _state_timedelta(
            self,
            from_state: SparkJobState,
            to_state: SparkJobState
    ) -> datetime.timedelta:
        self._require_minimum_state(to_state)
        if from_state not in self.state_timestamps:
            raise Exception(f'Cannot find timestamp for state {from_state} in {self}')
        if to_state not in self.state_timestamps:
            raise Exception(f'Cannot find timestamp for state {to_state} in {self}')
        return (
            self.state_timestamps[to_state]
            - self.state_timestamps[from_state]
        )

    def _require_state(self, state: SparkJobState) -> None:
        if self.state != state:
            raise Exception(f'Job is not in state {state}.')

    def _require_minimum_state(self, state: SparkJobState) -> None:
        if self.state < state:
            raise Exception(f'Job has not yet been in state {state}.')

    def _set_state(self, state: SparkJobState) -> None:
        if state is SparkJobState.RUNNING:
            self._log_start()
        elif state is SparkJobState.FINISHED:
            self._log_end()
        self._state = state
        self.state_timestamps[state] = datetime.datetime.now()

    def _log_start(self) -> None:
        self.logger.info(f'Starting: {self.name}')

    def _log_end(self) -> None:
        self.logger.info(f'Finished: {self.name}')


class SparkSQLJob(SparkJob, ABC):
    """
    Base class for all Spark jobs that use a template to generate
    declarative Spark SQL.
    """

    @property
    @abstractmethod
    def sql_template(self) -> str:
        """
        A Jinja2-compatible template that returns valid Spark SQL.
        Members of this class or subclasses may be accessed in the
        template through 'job._' which is a synonym for 'self._'.
        """
        pass

    @property
    def query(self) -> str:
        """
        The Spark SQL statement to be executed by this SparkSQLJob.
        """
        return jinja2.Template(self.sql_template).render(job=self)

    def execute_sql(self) -> DataFrame:
        """
        Execute the Spark SQL statement and return the resulting `DataFrame`.
        """
        try:
            self.logger.info(self.query)
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

    def execute(self) -> DataFrame:
        return self.execute_sql()
