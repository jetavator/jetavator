import os

from hashlib import md5
from base64 import b64encode


def generate_sql_password(seed=None):

    if seed is None:
        encoded_seed = os.urandom(64)
    else:
        encoded_seed = seed.encode("utf-8")

    return (
        b64encode(
            md5(encoded_seed).digest()
        )[0:22].decode("ascii", "ignore")
    )
