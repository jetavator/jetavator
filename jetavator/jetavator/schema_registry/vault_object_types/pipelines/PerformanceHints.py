import jsdom


class PerformanceHints(jsdom.Object):

    no_update_hubs: str = jsdom.Property(bool, default=False)
    no_update_links: str = jsdom.Property(bool, default=False)
    no_update_referenced_hubs: str = jsdom.Property(bool, default=False)
