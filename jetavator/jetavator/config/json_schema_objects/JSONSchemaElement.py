import jsonschema


class JSONSchemaElement(object):

    def __init__(self, *args, _document=None, **kwargs):
        self._document = _document or self

    @classmethod
    def _schema(cls):
        return {}

    def _validate(self):
        jsonschema.validate(instance=self, schema=self._schema())

    def _walk(self):
        yield self
