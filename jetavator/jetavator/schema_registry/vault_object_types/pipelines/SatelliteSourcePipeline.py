from typing import Dict, List

import wysdom

from .. import Source

from .SatellitePipeline import SatellitePipeline
from .SatellitePipelineDependency import SatellitePipelineDependency


class SatelliteSourcePipeline(
    SatellitePipeline,
    register_as="source"
):

    type: str = wysdom.UserProperty(wysdom.SchemaConst('source'))
    _source: str = wysdom.UserProperty(str, name="source")

    @property
    def key_columns(self) -> Dict[str, str]:
        if self._key_columns:
            return self._key_columns
        else:
            return {
                key_column: source_column
                for key_column, source_column in zip(
                    self.satellite.parent.hubs.keys(),
                    self.source.columns.keys()
                )
            }

    @property
    def source(self) -> Source:
        return self.project["source", self._source]

    @property
    def dependencies(self) -> List[SatellitePipelineDependency]:
        return [
            SatellitePipelineDependency(
                {'name': self._source, 'type': 'source'},
                json_dom_info=wysdom.dom.DOMInfo(
                    document=wysdom.document(self), parent=self)
            )
        ]
