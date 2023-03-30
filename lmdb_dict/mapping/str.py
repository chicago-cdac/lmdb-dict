import lmdb_dict.cache
import lmdb_dict.util

from . import abc


class StrLmdbDict(abc.LmdbDict):
    """Dictionary interface to LMDB supporting only str keys and values.

    LMDB stores keys and values as bytes. Keys and values provided to
    this dictionary are encoded/decoded from/to str.

    Only databases configured for non-duplicative keys are supported.

    For persistence of arbitrary serializable values see SafeLmdbDict.

    """
    __slots__ = ()

    encoding = 'utf-8'

    def __init__(self, *args, **kwargs):
        if 'cache' in kwargs:
            raise TypeError(f"'cache' is an invalid keyword argument for "
                            f"{self.__class__.__name__}()")

        super().__init__(*args, cache=lmdb_dict.cache.DummyCache, **kwargs)

    @classmethod
    def _deserialize_(cls, raw):
        return str(raw, encoding=cls.encoding)

    @classmethod
    def _serialize_(cls, value):
        try:
            bstr = lmdb_dict.util.BytesStr.make(value, encoding=cls.encoding)
        except lmdb_dict.util.BytesStr.BStrTypeError as exc:
            raise TypeError(f'{cls.__name__} {exc.message}') from None

        return bytes(bstr)
