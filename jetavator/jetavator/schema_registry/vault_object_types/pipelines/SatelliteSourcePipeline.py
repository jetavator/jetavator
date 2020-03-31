from ....sql_model import SatelliteSourcePipelineModel

from .SatellitePipeline import SatellitePipeline, SatellitePipelineDependency


class SatelliteSourcePipeline(
    SatellitePipeline,
    register_as="source",
    sql_model_class="satellite_source_pipeline"
):

    required_yaml_properties = ["source"]

    optional_yaml_properties = []

    @property
    def key_columns(self):
        if "key_columns" in self.definition:
            return self.definition["key_columns"]
        elif self.satellite.parent.type == "hub":
            return {
                self.satellite.parent.name: list(self.source.columns.keys())[0]
            }
        elif self.satellite.parent.type == "link":
            return {
                x[0]: x[1]
                for x in zip(
                    list(self.satellite.parent.link_hubs.keys()),
                    list(self.source.columns.keys())[
                        :len(self.satellite.parent.link_hubs)])
                }
        else:
            raise Exception(
                "Unexpected value for satellite.parent.type: "
                f"{satellite.parent.type}"
            )

    @property
    def source(self):
        return self.project["source", self.definition["source"]]

    @property
    def dependencies(self):
        return [
            SatellitePipelineDependency(
                self.project,
                self,
                {'name': self.definition["source"], 'type': 'source'}
            )
        ]
