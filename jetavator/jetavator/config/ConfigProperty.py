class ConfigProperty(object):

    def __init__(self, name, default):
        self.name = name
        self.default = default

    def __get__(self, obj, objtype):
        if callable(self.default) and self.name not in obj:
            obj[self.name] = self.default(obj)
            return obj[self.name]
        else:
            return obj.get(self.name, self.default)

    def __set__(self, obj, value):
        obj[self.name] = value
