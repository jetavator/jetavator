from .__version__ import __version__
from .__required__ import REQUIRED
from .default_logger import DEFAULT_LOGGER_CONFIG
from .VaultAction import VaultAction
from .ServiceOwner import ServiceOwner
from .Engine import Engine, LoadType
from .config import Config

from pkg_resources import iter_entry_points as _iter_entry_points

plugins = {
    entry_point.name: entry_point.load()
    for entry_point
    in _iter_entry_points('jetavator.plugins')
}
