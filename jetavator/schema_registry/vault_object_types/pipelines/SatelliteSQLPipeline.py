from typing import Optional, Dict, List

import wysdom

from .SatellitePipeline import SatellitePipeline
from .SatellitePipelineDependency import SatellitePipelineDependency


class SatelliteSQLPipeline(
    SatellitePipeline,
    register_as="sql"
):

    type: str = wysdom.UserProperty(wysdom.SchemaConst('sql'))
    sql: str = wysdom.UserProperty(str, name="sql")
    load_dt: Optional[str] = wysdom.UserProperty(str, optional=True)
    deleted_ind: Optional[str] = wysdom.UserProperty(str, optional=True)
    dependencies: List[SatellitePipelineDependency] = wysdom.UserProperty(
        wysdom.SchemaArray(SatellitePipelineDependency), default=[])

    @property
    def key_columns(self) -> Dict[str, str]:
        if self._key_columns:
            return self._key_columns
        else:
            return {
                hub_alias: hub_alias
                for hub_alias in self.satellite.parent.hubs.keys()
            }
