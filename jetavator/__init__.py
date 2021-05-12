# TODO: Remove any unnecessary imports

from .__version__ import __version__
from .default_logger import DEFAULT_LOGGER_CONFIG
from .VaultAction import VaultAction
from jetavator.App import App
from jetavator.LoadType import LoadType
from .config import AppConfig

import jetavator.services
import jetavator.spark_services
import jetavator.engine

# TODO: Is this still needed?
from pkg_resources import iter_entry_points as _iter_entry_points

plugins = {
    entry_point.name: entry_point.load()
    for entry_point
    in _iter_entry_points('jetavator.plugins')
}
