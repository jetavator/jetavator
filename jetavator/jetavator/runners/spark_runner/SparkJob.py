from __future__ import annotations

import datetime
import jinja2

from logging import Logger
from typing import List

from abc import ABC, abstractmethod

from concurrent.futures import Future

from lazy_property import LazyProperty
from pyspark.sql import DataFrame, SparkSession

from jetavator.mixins import RegistersSubclasses

from .SparkRunnerABC import SparkRunnerABC
from .SparkJobABC import SparkJobABC
from .SparkJobState import SparkJobState

COALESCE_PARTITIONS = 10


class SparkJob(SparkJobABC, RegistersSubclasses, ABC):

    def __init__(self, runner: SparkRunnerABC, *args, **kwargs) -> None:
        super().__init__(runner, *args, **kwargs)
        self.runner = runner
        self.state_timestamps = {}
        self.state = None
        self.result = None
        self.template_arg_values = kwargs
        self.set_state(SparkJobState.BLOCKED)
        for arg, arg_name in zip(args, self.template_args):
            self.template_arg_values[arg_name] = arg
        for arg_name, value in self.template_arg_values.items():
            self.__setattr__(arg_name, value)

    @classmethod
    def get_or_create(cls, runner: SparkRunnerABC, *args) -> SparkJobABC:
        key = cls.construct_key(*args)
        if key not in runner.jobs:
            runner.jobs[key] = cls(runner, *args)
        return runner.jobs[key]

    @property
    def logger(self) -> Logger:
        return self.runner.logger

    @classmethod
    def construct_key(cls, *args) -> str:
        return '/'.join([
            getattr(cls, 'registered_name'),
            *[
                '.'.join(vault_object.key)
                for vault_object in args
            ]
        ])

    @LazyProperty
    def key(self) -> str:
        return self.construct_key(*[
            self.template_arg_values[arg]
            for arg in self.key_args
        ])

    @property
    def class_name(self) -> str:
        return type(self).__name__

    @property
    def primary_vault_object_key(self) -> str:
        return self.template_arg_values[self.key_args[0]].key

    @property
    def name(self) -> str:
        return jinja2.Template(self.name_template).render(
            self.template_arg_values
        )

    @property
    @abstractmethod
    def name_template(self) -> str:
        pass

    @property
    @abstractmethod
    def template_args(self) -> List[str]:
        pass

    @property
    def key_args(self) -> List[str]:
        return self.template_args

    @property
    def dependencies(self) -> List[SparkJob]:
        return []

    @property
    def spark(self) -> SparkSession:
        return self.runner.compute_service.spark

    def set_state(self, state: SparkJobState) -> None:
        if state is SparkJobState.RUNNING:
            self.logger.info(f'Starting: {self.name}')
        elif state is SparkJobState.FINISHED:
            self.logger.info(f'Finished: {self.name}')
        self.state = state
        self.state_timestamps[state] = datetime.datetime.now()

    def acknowledge(self) -> None:
        self.set_state(SparkJobState.ACKNOWLEDGED)

    def state_timedelta(
        self,
        from_state: SparkJobState,
        to_state: SparkJobState
    ) -> datetime.timedelta:
        if from_state not in self.state_timestamps:
            raise Exception(f'Cannot find timestamp for state {from_state} in {self}')
        if to_state not in self.state_timestamps:
            raise Exception(f'Cannot find timestamp for state {to_state} in {self}')
        return (
            self.state_timestamps[to_state]
            - self.state_timestamps[from_state]
        )

    @property
    def wait_time(self) -> datetime.timedelta:
        return self.state_timedelta(
            SparkJobState.BLOCKED, SparkJobState.READY
        )

    @property
    def queue_time(self) -> datetime.timedelta:
        return self.state_timedelta(
            SparkJobState.READY, SparkJobState.RUNNING
        )

    @property
    def execution_time(self) -> datetime.timedelta:
        return self.state_timedelta(
            SparkJobState.RUNNING, SparkJobState.FINISHED
        )

    @abstractmethod
    def execute(self) -> DataFrame:
        pass

    def require_state(self, state: SparkJobState) -> None:
        if self.state != state:
            raise Exception(f'Job is not in state {state}.')

    def run(self) -> None:
        self.require_state(SparkJobState.READY)
        self.set_state(SparkJobState.RUNNING)
        self.result = self.execute()
        if type(self.result) is not Future:
            self.set_state(SparkJobState.FINISHED)

    def check_if_blocked(self) -> bool:
        if self.state == SparkJobState.BLOCKED:
            if self.job_blocked:
                return False
            else:
                self.set_state(SparkJobState.READY)
                return True

    def check_if_finished(self) -> bool:
        self.require_state(SparkJobState.RUNNING)
        if self.check_if_job_complete():
            self.set_state(SparkJobState.FINISHED)
            return True

    def check_if_job_complete(self) -> bool:
        if type(self.result) is Future:
            if self.result.exception():
                raise Exception(self.result.exception())
            return self.result.done()
        else:
            # TODO: Remove this once non-async jobs are all gone!
            self.logger.warning(f'Non-async job {self.name}')
            return True

    @property
    def job_blocked(self) -> bool:
        return any([
            dependency.state in [
                SparkJobState.BLOCKED,
                SparkJobState.READY,
                SparkJobState.RUNNING
            ]
            for dependency in self.dependencies
        ])


class SparkSQLJob(SparkJob, ABC):

    @property
    @abstractmethod
    def sql_template(self) -> str:
        pass

    @property
    def query(self) -> str:
        return jinja2.Template(self.sql_template).render(
            self.template_arg_values
        )

    def execute_sql(self) -> DataFrame:
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

    def execute(self) -> DataFrame:
        return self.execute_sql()
