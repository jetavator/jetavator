import wysdom


class PerformanceHints(wysdom.UserObject):

    no_update_hubs: str = wysdom.UserProperty(bool, default=False)
    no_update_links: str = wysdom.UserProperty(bool, default=False)
    no_update_referenced_hubs: str = wysdom.UserProperty(bool, default=False)
