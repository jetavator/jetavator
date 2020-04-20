from __future__ import annotations  # Remove in Python 3.8

from logging import Logger
from typing import Dict, List
from lazy_property import LazyProperty

import pandas as pd

from jetavator.schema_registry import Source, Satellite, SatelliteOwner

from . import SparkJob
from .. import Runner

from .SparkJobState import SparkJobState

from .jobs import (
    CreateSource,
    DropSource,
    InputKeys,
    OutputKeys,
    ProducedKeys,
    SatelliteQuery,
    SerialiseSatellite,
    StarData,
    StarKeys,
    StarMerge
)


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


class SparkRunner(Runner, register_as="local_spark"):
    """
    Transforms the object definitions in a `Project` into a set of
    Spark jobs and enables them to be run in dependency order.
    """

    @property
    def logger(self) -> Logger:
        """
        Python `Logger` instance for raising log messages.
        """
        return self.engine.logger

    @LazyProperty
    def jobs(self) -> Dict[str, SparkJob]:
        """
        Dictionary of all Spark jobs in the SparkRunner, indexed by unique
        string representation of the job key.
        """
        return {
            str(job.key): job
            for job in self._create_jobs()
        }

    @property
    def blocked_jobs(self) -> Dict[str, SparkJob]:
        """
        Subset of `jobs` that are blocked from running by other jobs
        that they depend on.
        """
        return self.jobs_in_state([SparkJobState.BLOCKED])

    @property
    def ready_jobs(self) -> Dict[str, SparkJob]:
        """
        Subset of `jobs` whose dependencies have all finished and are
        ready to start running.
        """
        return self.jobs_in_state([SparkJobState.READY])

    @property
    def running_jobs(self) -> Dict[str, SparkJob]:
        """
        Subset of `jobs` that are currently running.
        """
        return self.jobs_in_state([SparkJobState.RUNNING])

    @property
    def finished_jobs(self) -> Dict[str, SparkJob]:
        """
        Subset of `jobs` that have recently finished, but are not yet
        acknowledged by the SparkRunner.
        """
        return self.jobs_in_state([SparkJobState.FINISHED])

    @property
    def acknowledged_jobs(self) -> Dict[str, SparkJob]:
        """
        Subset of `jobs` that are finished and acknowledged.
        """
        return self.jobs_in_state([SparkJobState.ACKNOWLEDGED])

    def get_job(
            self,
            registered_name: str,
            *args: VaultObject
    ) -> SparkJob:
        """
        :param registered_name: The registered subclass name of the job.
        :param args:            A variable-length list of `VaultObject` instances
                                associated with the SparkJob. The number and types
                                of these objects depends on the subclass
                                implementation.
        :return:                An existing SparkJob matching the supplied key values.
        """
        return self.jobs[SparkJob.spark_job_key(registered_name, *args)]

    def run(self) -> None:
        """
        Run the workload management algorithm to run all the Spark jobs
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

    def jobs_in_state(self, states: List[SparkJobState]) -> Dict[str, SparkJob]:
        """
        Return a dictionary of Spark jobs in a list of states.

        :param states: A list of `SparkJobState`s to search for.
        """
        return {
            name: job
            for name, job in self.jobs.items()
            if job.state in states
        }

    def performance_data(self) -> pd.DataFrame:
        """
        A Pandas `DataFrame` containing the queue, wait and execution
        time for each Spark job.
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

    def _create_jobs(self) -> List[SparkJob]:
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

    def _create_source_jobs(self, source: Source) -> List[SparkJob]:
        jobs = [
            CreateSource(self, source)
        ]
        if self.compute_service.source_csv_exists(source):
            jobs += [
                DropSource(self, source)
            ]
        return jobs

    def _create_satellite_jobs(self, satellite: Satellite) -> List[SparkJob]:
        return [
            *InputKeys.keys_for_satellite(self, satellite),
            SatelliteQuery(self, satellite),
            *ProducedKeys.keys_for_satellite(self, satellite),
            *OutputKeys.keys_for_satellite(self, satellite),
            SerialiseSatellite(self, satellite)
        ]

    def _create_star_jobs(self, satellite_owner: SatelliteOwner) -> List[SparkJob]:
        return [
            StarKeys(self, satellite_owner),
            StarData(self, satellite_owner),
            StarMerge(self, satellite_owner)
        ]

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
