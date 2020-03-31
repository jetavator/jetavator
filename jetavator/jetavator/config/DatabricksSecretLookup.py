from jetavator.config.SecretLookup import SecretLookup

import lazy_property


class DatabricksSecretLookup(SecretLookup, register_as='databricks'):

    @lazy_property.LazyProperty
    def dbutils(self):
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]

    def lookup_secret(self, secret_name):
        return self.dbutils.secrets.get(scope="jetavator", key=secret_name)
