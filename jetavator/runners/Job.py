from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Any

import datetime

from logging import Logger
from concurrent.futures import Future
from wysdom.mixins import RegistersSubclasses
from lazy_property import LazyProperty

from jetavator.schema_registry import VaultObject, Satellite
from .JobState import JobState
from jetavator.runners.RunnerABC import RunnerABC


class Job(RegistersSubclasses, ABC):
    """
    Base class for all jobs.

    :param runner: A `Runner` which is for providing execution services
                   and allowing Jobs to look up other Jobs for
                   dependency purposes.

    :param *args:  A variable-length list of `VaultObject` instances associated
                   with the Job. The number and types of these objects
                   depends on the subclass implementation. The combination of
                   VaultObjects supplied, together with the registered name of
                   the Job subclass, must constitute a unique key for that
                   Job instance.
    """

    @abstractmethod
    def __init__(self, runner: RunnerABC, *args: VaultObject) -> None:
        """
        Abstract class constructor to be extended and called by subclasses.
        """
        super().__init__()
        self.runner = runner
        self.state_timestamps = {}
        self.result = None
        self._vault_objects = args
        self._state = None
        self._set_state(JobState.BLOCKED)

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
        The unique key for this Job, based on its class and arguments.
        """
        return self.job_key(
            self.registered_name,
            *self._vault_objects
        )

    @staticmethod
    def job_key(
            registered_name: str,
            *args: VaultObject
    ):
        return '/'.join([
            registered_name,
            *[
                '.'.join(vault_object.key)
                for vault_object in args
            ]
        ])

    @classmethod
    def keys_for_satellite(
            cls,
            runner: RunnerABC,
            satellite: Satellite
    ) -> List[Job]:
        return []

    @property
    def primary_vault_object(self) -> VaultObject:
        """
        The first VaultObject argument for this Job. Used for reporting.
        """
        return self._vault_objects[0]

    @property
    @abstractmethod
    def name(self) -> str:
        """
        The name of this Job instance for logging purposes. If the
        subclass is also a view, this also becomes the view name.
        """
        pass

    @property
    def dependencies(self) -> List[Job]:
        """
        A list of Jobs that must be executed before this Job
        can start.
        """
        return []

    @property
    def state(self) -> JobState:
        """
        The current execution state of the Job.
        """
        return self._state

    @property
    def wait_time(self) -> datetime.timedelta:
        """
        The time this job spent being blocked by other jobs before
        being ready to execute. Requires a minimum state of
        `JobState.READY`.
        """
        return self._state_timedelta(
            JobState.BLOCKED, JobState.READY
        )

    @property
    def queue_time(self) -> datetime.timedelta:
        """
        The time this job spent between being ready to execute and starting
         to execute. Requires a minimum state of `JobState.RUNNING`.
        """
        return self._state_timedelta(
            JobState.READY, JobState.RUNNING
        )

    @property
    def execution_time(self) -> datetime.timedelta:
        """
        The time this job spent being executed. Requires a minimum state of
        `JobState.FINISHED`.
        """
        return self._state_timedelta(
            JobState.RUNNING, JobState.FINISHED
        )

    @abstractmethod
    def execute(self) -> Any:
        """
        Execute the underlying job and return the resultant object.
        """
        pass

    def check_if_ready(self) -> bool:
        """
        Check if a blocked job is now ready by examining the state of its
        dependencies. If it is, update its state to `JobState.READY`
        and return True.
        """
        if self.state == JobState.BLOCKED:
            if self._has_blocking_dependencies:
                return False
            else:
                self._set_state(JobState.READY)
                return True

    def run(self) -> None:
        """
        Start executing a ready job and update its state to
        `JobState.RUNNING`.
        """
        self._require_state(JobState.READY)
        self._set_state(JobState.RUNNING)
        self.result = self.execute()
        if type(self.result) is not Future:
            self._set_state(JobState.FINISHED)

    def check_if_finished(self) -> bool:
        """
        Check if a running job is now finished by examining its execution
        state. If it is, update its state to `JobState.FINISHED`
        and return True.
        """
        self._require_state(JobState.RUNNING)
        if self._check_if_execution_complete():
            self._set_state(JobState.FINISHED)
            return True

    def acknowledge(self) -> None:
        """
        Acknowledge a completed job. Does nothing except to remove it
        from the list of recently finished jobs
        (see `Runner.finished_jobs`)
        """
        self._require_state(JobState.FINISHED)
        self._set_state(JobState.ACKNOWLEDGED)

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
                JobState.BLOCKED,
                JobState.READY,
                JobState.RUNNING
            ]
            for dependency in self.dependencies
        ])

    def _state_timedelta(
            self,
            from_state: JobState,
            to_state: JobState
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

    def _require_state(self, state: JobState) -> None:
        if self.state != state:
            raise Exception(f'Job is not in state {state}.')

    def _require_minimum_state(self, state: JobState) -> None:
        if self.state < state:
            raise Exception(f'Job has not yet been in state {state}.')

    def _set_state(self, state: JobState) -> None:
        if state is JobState.RUNNING:
            self._log_start()
        elif state is JobState.FINISHED:
            self._log_end()
        self._state = state
        self.state_timestamps[state] = datetime.datetime.now()

    def _log_start(self) -> None:
        self.logger.info(f'Starting: {self.name}')

    def _log_end(self) -> None:
        self.logger.info(f'Finished: {self.name}')
