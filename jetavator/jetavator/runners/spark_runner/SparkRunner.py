from __future__ import annotations  # Remove in Python 3.8

from logging import Logger
from typing import Dict, List

import pandas as pd

from jetavator import KeyType
from jetavator.schema_registry import Source, Satellite, SatelliteOwner

from . import SparkJob, SparkRunnerABC

from .SparkJobState import SparkJobState

from .jobs import (
    CreateSource,
    DropSource,
    InputKeys,
    OutputKeys,
    ProducedHubKeys,
    ProducedLinkKeys,
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


# TODO: Remove dependency of SparkJob on SparkRunner (and need for
#       separate interface classes) - e.g. by having SparkJob
#       subclasses return list of dependency keys

class SparkRunner(SparkRunnerABC, register_as="local_spark"):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._jobs = {
            job.key: job
            for job in self.create_jobs()
        }

    @property
    def logger(self) -> Logger:
        return self.engine.logger

    @property
    def jobs(self) -> Dict[str, SparkJob]:
        return self._jobs

    def create_jobs(self) -> List[SparkJob]:
        return [
            job
            for source in self.project.sources.values()
            for job in self.source_jobs(source)
        ] + [
            job
            for satellite in self.project.satellites.values()
            for job in self.satellite_jobs(satellite)
        ] + [
            job
            for satellite_owner in self.project.satellite_owners.values()
            if satellite_owner.satellites_containing_keys
            for job in self.star_jobs(satellite_owner)
        ]

    def jobs_in_state(self, states) -> Dict[str, SparkJob]:
        return {
            name: job
            for name, job in self._jobs.items()
            if job.state in states
        }

    def source_jobs(self, source: Source) -> List[SparkJob]:
        jobs = [
            CreateSource(self, source)
        ]
        if self.compute_service.source_csv_exists(source):
            jobs += [
                DropSource(self, source)
            ]
        return jobs

    def satellite_jobs(self, satellite: Satellite) -> List[SparkJob]:
        return [
            *InputKeys.keys_for_satellite(self, satellite, KeyType.HUB),
            *InputKeys.keys_for_satellite(self, satellite, KeyType.LINK),
            SatelliteQuery(self, satellite),
            *ProducedHubKeys.keys_for_satellite(self, satellite),
            *ProducedLinkKeys.keys_for_satellite(self, satellite),
            *OutputKeys.keys_for_satellite(self, satellite, KeyType.HUB),
            *OutputKeys.keys_for_satellite(self, satellite, KeyType.LINK),
            SerialiseSatellite(self, satellite)
        ]

    # TODO: Eliminate StarMerge if star datastore is external to Spark
    def star_jobs(self, satellite_owner: SatelliteOwner) -> List[SparkJob]:
        return [
            StarKeys(self, satellite_owner),
            StarData(self, satellite_owner),
            StarMerge(self, satellite_owner)
        ]

    @property
    def blocked_jobs(self) -> Dict[str, SparkJob]:
        return self.jobs_in_state([SparkJobState.BLOCKED])

    @property
    def ready_jobs(self) -> Dict[str, SparkJob]:
        return self.jobs_in_state([SparkJobState.READY])

    @property
    def running_jobs(self) -> Dict[str, SparkJob]:
        return self.jobs_in_state([SparkJobState.RUNNING])

    @property
    def finished_jobs(self) -> Dict[str, SparkJob]:
        return self.jobs_in_state([SparkJobState.FINISHED])

    @property
    def acknowledged_jobs(self) -> Dict[str, SparkJob]:
        return self.jobs_in_state([SparkJobState.ACKNOWLEDGED])

    def run(self) -> None:
        self.start_ready_jobs()
        if not self.running_jobs and not self.finished_jobs:
            raise Exception("Dependency error. No jobs could be started.")
        while self.running_jobs or self.finished_jobs:
            self.check_for_finished_jobs()
            if self.finished_jobs:
                self.acknowledge_finished_jobs()
                self.start_ready_jobs()

    def check_for_finished_jobs(self) -> None:
        for job in self.running_jobs.values():
            job.check_if_finished()

    def acknowledge_finished_jobs(self) -> None:
        for job in self.finished_jobs.values():
            job.acknowledge()

    def start_ready_jobs(self) -> None:
        self.find_ready_jobs()
        for job in self.ready_jobs.values():
            job.run()

    def find_ready_jobs(self) -> None:
        for job in self.blocked_jobs.values():
            job.check_if_blocked()

    def performance_data(self) -> pd.DataFrame:
        return pd.DataFrame([
            (
                key,
                '.'.join(job.primary_vault_object_key),
                job.class_name,
                job.queue_time,
                job.wait_time,
                job.execution_time.total_seconds()
            )
            for key, job in self._jobs.items()
        ], columns=[
            'key',
            'primary_vault_object_key',
            'class_name',
            'queue_time',
            'wait_time',
            'execution_time'
        ])
