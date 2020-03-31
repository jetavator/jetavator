from .JSONSchemaElement import JSONSchemaElement


class JSONSchemaConstMeta(type):

    def __getitem__(cls, value):
        return type(
            f'{cls.__name__}_{value}',
            (cls,),
            {'value': value}
        )


class JSONSchemaConst(JSONSchemaElement, metaclass=JSONSchemaConstMeta):

    def __new__(self, value, _document=None):
        if value != self.value:
            raise ValueError(f'Constant can only take value "{value}".')
        return self.value

    @classmethod
    def _schema(cls):
        return {'const': cls.value}


def const(value):
    return JSONSchemaConst[value]
