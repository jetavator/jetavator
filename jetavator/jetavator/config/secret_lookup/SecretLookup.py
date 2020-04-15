import re

from jetavator.mixins import RegistersSubclasses


class SecretLookup(RegistersSubclasses):

    match_pattern = r"\$(?P<value>\w*)"

    def get_secret_name(self, value):
        match = (
            re.match(self.match_pattern, value)
            if isinstance(value, str)
            else None
        )
        return match['value'] if match else None

    def lookup_secret(self, secret_name):
        return secret_name

    def __call__(self, value):
        secret_name = self.get_secret_name(value)
        if secret_name:
            return self.lookup_secret(secret_name)
        else:
            return value
