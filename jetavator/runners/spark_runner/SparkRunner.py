from __future__ import annotations  # Remove in Python 3.8

from .. import Runner
from . import SparkJob


class SparkRunner(Runner, register_as="spark"):
    """
    Transforms the object definitions in a `Project` into a set of
    Spark jobs and enables them to be run in dependency order.
    """
    job_class: SparkJob = SparkJob
