from jetavator import json_schema_objects as jso


class PerformanceHints(jso.Object):

    no_update_hubs: str = jso.Property(bool, default=False)
    no_update_links: str = jso.Property(bool, default=False)
    no_update_referenced_hubs: str = jso.Property(bool, default=False)
