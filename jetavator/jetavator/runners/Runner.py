from __future__ import annotations  # Remove in Python 3.8

from abc import ABC

import pandas as pd

from logging import Logger
from typing import Dict, List, Type
from lazy_property import LazyProperty
from wysdom.mixins import RegistersSubclasses

from jetavator import Engine
from jetavator.schema_registry import Project, Source, Satellite, SatelliteOwner, VaultObject
from jetavator.services import ComputeService

from .Job import Job
from .JobState import JobState
from jetavator.runners.RunnerABC import RunnerABC


class Runner(RegistersSubclasses, RunnerABC, ABC):
    job_class: Job = Job

    def __init__(
            self,
            engine: Engine,
            compute_service: ComputeService,
            project: Project
    ):
        """
        Default constructor to be inherited by subclasses.
        Not intended for direct use: use
        :py:meth:`from_compute_service` instead.
        """
        super().__init__()
        self.engine = engine
        self.compute_service = compute_service
        self.project = project

    @classmethod
    def from_compute_service(
            cls,
            engine: Engine,
            compute_service: ComputeService,
            project: Project
    ) -> Runner:
        """
        Constructor that takes an :py:class:`~jetavator.Engine`,
        a :py:class:`~jetavator.services.ComputeService` and
        a :py:class:`~jetavator.schema_registry.Project`
        and returns a registered subclass
        of `Runner` as specified in `compute_service.config.type`
        """
        return cls.registered_subclass_instance(
            compute_service.config.type,
            engine,
            compute_service,
            project
        )

    @property
    def logger(self) -> Logger:
        """
        Python `Logger` instance for raising log messages.
        """
        return self.engine.logger

    @LazyProperty
    def jobs(self) -> Dict[str, Job]:
        """
        Dictionary of all Jobs in the Runner, indexed by unique
        string representation of the job key.
        """
        return {
            str(job.key): job
            for job in self._create_jobs()
        }

    @property
    def blocked_jobs(self) -> Dict[str, Job]:
        """
        Subset of `jobs` that are blocked from running by other jobs
        that they depend on.
        """
        return self.jobs_in_state([JobState.BLOCKED])

    @property
    def ready_jobs(self) -> Dict[str, Job]:
        """
        Subset of `jobs` whose dependencies have all finished and are
        ready to start running.
        """
        return self.jobs_in_state([JobState.READY])

    @property
    def running_jobs(self) -> Dict[str, Job]:
        """
        Subset of `jobs` that are currently running.
        """
        return self.jobs_in_state([JobState.RUNNING])

    @property
    def finished_jobs(self) -> Dict[str, Job]:
        """
        Subset of `jobs` that have recently finished, but are not yet
        acknowledged by the Runner.
        """
        return self.jobs_in_state([JobState.FINISHED])

    @property
    def acknowledged_jobs(self) -> Dict[str, Job]:
        """
        Subset of `jobs` that are finished and acknowledged.
        """
        return self.jobs_in_state([JobState.ACKNOWLEDGED])

    def get_job(
            self,
            registered_name: str,
            *args: VaultObject
    ) -> Job:
        """
        :param registered_name: The registered subclass name of the job.
        :param args:            A variable-length list of `VaultObject` instances
                                associated with the Job. The number and types
                                of these objects depends on the subclass
                                implementation.
        :return:                An existing Job matching the supplied key values.
        """
        return self.jobs[Job.job_key(registered_name, *args)]

    def run(self) -> None:
        """
        Run the workload management algorithm to run all the jobs
        in dependency order.
        """
        self._start_ready_jobs()
        if not self.running_jobs and not self.finished_jobs:
            raise RuntimeError("Dependency error. No jobs could be started.")
        while self.running_jobs or self.finished_jobs:
            self._check_for_finished_jobs()
            if self.finished_jobs:
                self._acknowledge_finished_jobs()
                self._start_ready_jobs()

    def jobs_in_state(self, states: List[JobState]) -> Dict[str, Job]:
        """
        Return a dictionary of jobs in a list of states.

        :param states: A list of `JobState`s to search for.
        """
        return {
            name: job
            for name, job in self.jobs.items()
            if job.state in states
        }

    def performance_data(self) -> pd.DataFrame:
        """
        A Pandas `DataFrame` containing the queue, wait and execution
        time for each job.
        """
        return pd.DataFrame([
            (
                key,
                '.'.join(job.primary_vault_object.key),
                job.registered_name,
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

    def _check_for_finished_jobs(self) -> None:
        for job in self.running_jobs.values():
            job.check_if_finished()

    def _acknowledge_finished_jobs(self) -> None:
        for job in self.finished_jobs.values():
            job.acknowledge()

    def _start_ready_jobs(self) -> None:
        self._find_ready_jobs()
        for job in self.ready_jobs.values():
            job.run()

    def _find_ready_jobs(self) -> None:
        for job in self.blocked_jobs.values():
            job.check_if_ready()

    def _create_jobs(self) -> List[Job]:
        return [
           job
           for source in self.project.sources.values()
           for job in self._create_source_jobs(source)
        ] + [
           job
           for satellite in self.project.satellites.values()
           for job in self._create_satellite_jobs(satellite)
        ] + [
           job
           for satellite_owner in self.project.satellite_owners.values()
           if satellite_owner.satellites_containing_keys
           for job in self._create_star_jobs(satellite_owner)
        ]

    def _job_class_by_name(self, name: str) -> Type[Job]:
        return self.job_class.registered_subclass(name)

    def _create_source_jobs(self, source: Source) -> List[Job]:
        jobs = [
            self._job_class_by_name("create_source")(self, source)
        ]
        if self.compute_service.source_csv_exists(source.name):
            jobs += [
                self._job_class_by_name("drop_source")(self, source)
            ]
        return jobs

    def _create_satellite_jobs(self, satellite: Satellite) -> List[Job]:
        return [
            *self._job_class_by_name("input_keys").keys_for_satellite(self, satellite),
            self._job_class_by_name("satellite_query")(self, satellite),
            *self._job_class_by_name("produced_keys").keys_for_satellite(self, satellite),
            *self._job_class_by_name("output_keys").keys_for_satellite(self, satellite),
            self._job_class_by_name("serialise_satellite")(self, satellite)
        ]

    def _create_star_jobs(self, satellite_owner: SatelliteOwner) -> List[Job]:
        return [
            self._job_class_by_name("star_keys")(self, satellite_owner),
            self._job_class_by_name("star_data")(self, satellite_owner),
            self._job_class_by_name("star_merge")(self, satellite_owner)
        ]
