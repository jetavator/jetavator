import os

from .SecretLookup import SecretLookup


class EnvironmentSecretLookup(SecretLookup, register_as='environment'):

    def lookup_secret(self, secret_name):
        return os.getenv(secret_name)
