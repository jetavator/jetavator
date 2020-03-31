import jinja2

from ....sql_model import SatelliteSQLPipelineModel

from .SatellitePipeline import SatellitePipeline, SatellitePipelineDependency


class SatelliteSQLPipeline(
    SatellitePipeline,
    register_as="sql",
    sql_model_class="satellite_sql_pipeline"
):

    required_yaml_properties = ["sql"]

    optional_yaml_properties = ["load_dt", "deleted_ind", "dependencies"]

    @property
    def sql(self):
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
        return jinja2.Template(self.definition["sql"]).render(table_aliases)

    @property
    def key_columns(self):
        if "key_columns" in self.definition:
            return self.definition["key_columns"]
        elif self.satellite.parent.type == "hub":
            return {
                self.satellite.parent.name: self.satellite.parent.name
            }
        elif self.satellite.parent.type == "link":
            return {
                hub_alias: hub_alias
                for hub_alias in self.satellite.parent.link_hubs.keys()
            }
        else:
            raise Exception(
                "Unexpected value for satellite.parent.type: "
                f"{satellite.parent.type}"
            )

    @property
    def dependencies(self):
        return [
            SatellitePipelineDependency(self.project, self, dependency)
            for dependency in
            self.definition.get("dependencies", [])
        ]
