from jetavator import json_schema_objects as jso


class PerformanceHints(jso.Object):

    no_update_hubs: str = jso.Property(jso.Boolean, default=False)
    no_update_links: str = jso.Property(jso.Boolean, default=False)
    no_update_referenced_hubs: str = jso.Property(jso.Boolean, default=False)
