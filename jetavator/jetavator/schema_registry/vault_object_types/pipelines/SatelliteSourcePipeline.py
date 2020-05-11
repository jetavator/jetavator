from typing import Dict, List

import jsdom

from .. import Source

from .SatellitePipeline import SatellitePipeline
from .SatellitePipelineDependency import SatellitePipelineDependency


class SatelliteSourcePipeline(
    SatellitePipeline,
    register_as="source"
):

    type: str = jsdom.Property(jsdom.Const('source'))
    _source: str = jsdom.Property(str, name="source")

    # TODO: Refactor this property to make it more readable (if it's still needed)
    @property
    def key_columns(self) -> Dict[str, str]:
        if self._key_columns:
            return self._key_columns
        elif self.satellite.parent.type == "hub":
            return {
                self.satellite.parent.name: list(self.source.columns.keys())[0]
            }
        elif self.satellite.parent.type == "link":
            return {
                x[0]: x[1]
                for x in zip(
                    list(self.satellite.parent.hubs.keys()),
                    list(self.source.columns.keys())[
                        :len(self.satellite.parent.hubs)])
                }
        else:
            raise Exception(
                "Unexpected value for satellite.parent.type: "
                f"{satellite.parent.type}"
            )

    @property
    def source(self) -> Source:
        return self.project["source", self._source]

    @property
    def dependencies(self) -> List[SatellitePipelineDependency]:
        return [
            SatellitePipelineDependency(
                {'name': self._source, 'type': 'source'},
                _dom_info=jsdom.DOMInfo(document=jsdom.document(self), parent=self)
            )
        ]
