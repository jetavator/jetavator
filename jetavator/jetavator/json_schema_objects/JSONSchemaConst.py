from abc import ABCMeta

from .JSONSchemaElement import JSONSchemaElement, JSONSchemaDOMInfo


class JSONSchemaConstMeta(ABCMeta):

    def __getitem__(cls, value):
        return type(
            f'{cls.__name__}_{value}',
            (cls,),
            {'value': value}
        )


class JSONSchemaConst(JSONSchemaElement, metaclass=JSONSchemaConstMeta):

    def __new__(
            cls,
            value: str,
            dom_info: JSONSchemaDOMInfo = None,
            **kwargs
    ) -> None:
        if value != cls.value:
            raise ValueError(f'Constant can only take value "{value}".')
        return cls.value

    @classmethod
    def _schema(cls):
        return {'const': cls.value}


def const(value):
    return JSONSchemaConst[value]
