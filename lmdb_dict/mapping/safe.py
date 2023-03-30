import zlib

import yaml

from . import abc


class SafeLmdbDict(abc.LmdbDict):
    """Dictionary interface to LMDB.

    LMDB stores keys and values as bytes. Keys are encoded/decoded
    from/to str. Arbitrary values are serialized to and from YAML – as
    permitted by pyyaml safe_load and safe_dump – and the resulting
    bytes compressed via zlib.

    Only databases configured for non-duplicative keys are supported.

    """
    __slots__ = ()

    encoding = 'utf-8'

    @staticmethod
    def _deserialize_(raw):
        return yaml.safe_load(zlib.decompress(raw))

    @classmethod
    def _serialize_(cls, value):
        serialized = yaml.safe_dump(value, default_flow_style=True, encoding=cls.encoding)
        return zlib.compress(serialized, zlib.Z_BEST_SPEED)
