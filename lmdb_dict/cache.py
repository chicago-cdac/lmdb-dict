import collections.abc

from cachetools import LRUCache


class LRUCache128(LRUCache):
    """LRUCache discarding Least Recently-Used elements once there are
    128 of them.

    """
    def __init__(self):
        super().__init__(maxsize=128)


class DummyCache(collections.abc.MutableMapping):
    """Mutable mapping capable of storing zero items."""

    # for compatibility with cachetools mappings otherwise in use
    currsize = maxsize = 0

    def __delitem__(self, key):
        raise KeyError(key)

    def __getitem__(self, key):
        raise KeyError(key)

    def __iter__(self):
        yield from ()

    def __len__(self):
        return 0

    def __setitem__(self, key, value):
        pass
