from __future__ import annotations  # Remove in Python 3.8

from logging import Logger
from typing import Dict, List

import pandas as pd

from jetavator import KeyType
from jetavator.schema_registry import Source, Satellite, SatelliteOwner

from . import SparkJob, SparkJobABC, SparkRunnerABC

from .SparkJobState import SparkJobState
from .jobs import *

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
        self._jobs = {}
        self.create_jobs()

    @property
    def logger(self) -> Logger:
        return self.engine.logger

    @property
    def jobs(self) -> Dict[str, SparkJob]:
        return self._jobs

    def get_job(self, class_name: str, *args):
        key = '/'.join([
            class_name,
            *[
                '.'.join(vault_object.key)
                for vault_object in args
            ]
        ])
        return self._jobs[key]

    def create_jobs(self) -> None:
        for source in self.project.sources.values():
            self.source_jobs(source)
        for satellite in self.project.satellites.values():
            self.satellite_jobs(satellite)
        for satellite_owner in self.project.satellite_owners.values():
            if satellite_owner.satellites_containing_keys:
                self.star_jobs(satellite_owner)

    def jobs_in_state(self, states) -> Dict[str, SparkJob]:
        return {
            name: job
            for name, job in self._jobs.items()
            if job.state in states
        }

    def source_jobs(self, source: Source) -> List[SparkJobABC]:
        jobs = [
            CreateSource.get_or_create(self, source)
        ]
        if self.compute_service.source_csv_exists(source):
            jobs += [
                DropSource.get_or_create(self, source)
            ]
        return jobs

    def satellite_jobs(self, satellite: Satellite) -> List[SparkJob]:
        return [
            *self.input_keys(satellite, KeyType.HUB),
            *self.input_keys(satellite, KeyType.LINK),
            SatelliteQuery.get_or_create(self, satellite),
            *self.produced_hub_keys(satellite),
            *self.produced_link_keys(satellite),
            *self.output_keys(satellite, KeyType.HUB),
            *self.output_keys(satellite, KeyType.LINK),
            SerialiseSatellite.get_or_create(self, satellite)
        ]

    # TODO: Eliminate StarMerge if star datastore is external to Spark
    def star_jobs(self, satellite_owner: SatelliteOwner) -> List[SparkJobABC]:
        return [
            StarKeys.get_or_create(self, satellite_owner),
            StarData.get_or_create(self, satellite_owner),
            StarMerge.get_or_create(self, satellite_owner)
        ]

    def input_keys(
        self,
        satellite: Satellite,
        key_type: KeyType
    ) -> List[SparkJobABC]:
        # TODO: Implement KeyType throughout schema_registry to avoid .value conversions
        if type(key_type) is str:
            key_type = KeyType.HUB if key_type == 'hub' else KeyType.LINK
        return [
            InputKeys.get_or_create(self, satellite, satellite_owner)
            for satellite_owner in satellite.input_keys(key_type.value).values()
        ]

    def satellite_owner_output_keys(
        self,
        satellite: Satellite,
        satellite_owner: SatelliteOwner,
        key_type: KeyType
    ) -> SparkJobABC:
        # TODO: Implement KeyType throughout schema_registry to avoid .value conversions
        if type(key_type) is str:
            key_type = KeyType.HUB if key_type == 'hub' else KeyType.LINK
        if satellite_owner.name not in satellite.input_keys(key_type.value):
            job_class = OutputKeysFromSatellite
        elif satellite_owner.name not in satellite.produced_keys(key_type.value):
            job_class = OutputKeysFromDependencies
        else:
            job_class = OutputKeysFromBoth
        return job_class.get_or_create(self, satellite, satellite_owner)

    def output_keys(
        self,
        satellite: Satellite,
        key_type: KeyType
    ) -> List[SparkJobABC]:
        # TODO: Implement KeyType throughout schema_registry to avoid .value conversions
        if type(key_type) is str:
            key_type = KeyType.HUB if key_type == 'hub' else KeyType.LINK
        return [
            self.satellite_owner_output_keys(satellite, satellite_owner, key_type)
            for satellite_owner in satellite.output_keys(key_type.value).values()
        ]

    def produced_hub_keys(self, satellite: Satellite) -> List[SparkJobABC]:
        return [
            ProducedHubKeys.get_or_create(
                self,
                satellite,
                hub
            )
            for hub in satellite.produced_keys('hub').values()
        ]

    def produced_link_keys(self, satellite: Satellite) -> List[SparkJobABC]:
        return [
            ProducedLinkKeys.get_or_create(
                self,
                satellite,
                link
            )
            for link in satellite.produced_keys('link').values()
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
