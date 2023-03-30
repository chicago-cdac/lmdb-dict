import lmdb_dict.cache

from . import safe


class CachedLmdbDict(safe.SafeLmdbDict):
    """Dictionary interface to LMDB which caches deserialized values.

    LMDB stores keys and values as bytes. Keys are encoded/decoded
    from/to str. Arbitrary values are serialized to and from YAML – as
    permitted by pyyaml safe_load and safe_dump – and the resulting
    bytes compressed via zlib.

    Deserialized values are cached by default using an LRU cache with a
    maximum size of 128 items.

    Only databases configured for non-duplicative keys are supported.

    """
    __slots__ = ()

    def __init__(self, *args, cache=lmdb_dict.cache.LRUCache128, **kwargs):
        super().__init__(*args, cache=cache, **kwargs)

        if getattr(self._locker_.cache, 'maxsize', None) == 0:
            raise TypeError(f'{self.__class__.__name__} supplied with non-caching '
                            f'entity or factory {cache}')
