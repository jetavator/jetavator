from hashlib import sha1

import json
import binascii


def dict_checksum(object_to_checksum):
    return binascii.hexlify(
        sha1(
            json.dumps(object_to_checksum, sort_keys=True).encode()
        ).digest()
    ).decode("ascii")
