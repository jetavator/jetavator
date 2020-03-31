from itertools import groupby


def sort_and_group_by(items, *args, **kwargs):
    items.sort(*args, **kwargs)
    return groupby(items, *args, **kwargs)
