from .JSONSchemaElement import JSONSchemaElement


class JSONSchemaPrimitive(JSONSchemaElement):

    def __new__(cls, value, _document=None):
        return cls.python_primitive(value)

    def __init_subclass__(cls, python_primitive, type_name):
        cls.python_primitive = python_primitive
        cls.type_name = type_name

    @classmethod
    def _schema(cls):
        return {'type': cls.type_name}


class JSONSchemaString(
    JSONSchemaPrimitive,
    python_primitive=str,
    type_name='string'
):
    pass


class JSONSchemaBool(
    JSONSchemaPrimitive,
    python_primitive=bool,
    type_name='boolean'
):
    pass


class JSONSchemaInt(
    JSONSchemaPrimitive,
    python_primitive=int,
    type_name='integer'
):
    pass


class JSONSchemaFloat(
    JSONSchemaPrimitive,
    python_primitive=float,
    type_name='number'
):
    pass


class JSONSchemaNone(JSONSchemaElement):

    def __new__(self, value, _document=None):
        if self.value is not None:
            raise ValueError('Value can only be None.')
        return None

    @classmethod
    def _schema(cls):
        return {'type': 'null'}
