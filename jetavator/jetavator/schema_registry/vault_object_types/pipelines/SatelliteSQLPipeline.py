from typing import Optional, Dict, List

import jinja2

import jsdom

from .SatellitePipeline import SatellitePipeline
from .SatellitePipelineDependency import SatellitePipelineDependency


class SatelliteSQLPipeline(
    SatellitePipeline,
    register_as="sql"
):

    type: str = jsdom.Property(jsdom.Const('sql'))
    _sql: str = jsdom.Property(str, name="sql")
    # TODO: Allow to be None
    load_dt: Optional[str] = jsdom.Property(str, default="")
    # TODO: Allow to be None
    deleted_ind: Optional[str] = jsdom.Property(str, default="")
    dependencies: List[SatellitePipelineDependency] = jsdom.Property(
        jsdom.List(SatellitePipelineDependency), default=[])

    @property
    def sql(self) -> str:
        # refactor SparkRunner classes into schema_registry object model
        # and refer to them here, so these temp table names are defined in
        # one place only
        table_aliases = {
            'source': {
                source.name: f'source_{source.name}'
                for source in self.project.sources.values()
            },
            'hub': {
                hub.name: {
                    'current': f'vault_{hub.full_name}',
                    'updates': (
                        'vault_updates'
                        f'_{hub.full_name}'
                        f'_{self.satellite.full_name}'
                    ),
                }
                for hub in self.project.hubs.values()
            },
            'link': {
                link.name: {
                    'current': f'vault_{link.full_name}',
                    'updates': (
                        'vault_updates'
                        f'_{link.full_name}'
                        f'_{self.satellite.full_name}'
                    ),
                }
                for link in self.project.links.values()
            },
            'satellite': {
                satellite.name: {
                    'current': f'vault_now_{satellite.name}',
                    'updates': f'vault_updates_{satellite.full_name}',
                    'history': f'vault_history_{satellite.name}'
                }
                for satellite in self.project.satellites.values()
            }
        }
        return jinja2.Template(self._sql).render(table_aliases)

    # TODO: Refactor to make this more readable (if it's still needed)
    @property
    def key_columns(self) -> Dict[str, str]:
        if self._key_columns:
            return self._key_columns
        elif self.satellite.parent.type == "hub":
            return {
                self.satellite.parent.name: self.satellite.parent.name
            }
        elif self.satellite.parent.type == "link":
            return {
                hub_alias: hub_alias
                for hub_alias in self.satellite.parent.hubs.keys()
            }
        else:
            raise Exception(
                "Unexpected value for satellite.parent.type: "
                f"{satellite.parent.type}"
            )
