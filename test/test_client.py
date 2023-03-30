import pytest

from lmdb_dict import SafeLmdbDict
from lmdb_dict.cache import DummyCache, LRUCache128
from lmdb_dict.util import DummyLockPool


def test_sharing(tmp_path_factory):
    a = tmp_path_factory.mktemp('a')
    b = tmp_path_factory.mktemp('b')
    c = tmp_path_factory.mktemp('c')

    a0 = SafeLmdbDict(a, cache=LRUCache128)
    a1 = SafeLmdbDict(a, cache=LRUCache128)

    b = SafeLmdbDict(b, cache=LRUCache128)

    c00 = SafeLmdbDict(c, '0', cache=LRUCache128, max_dbs=3)
    c01 = SafeLmdbDict(c, '0', cache=LRUCache128, max_dbs=3)
    c10 = SafeLmdbDict(c, '1', cache=LRUCache128, max_dbs=3)

    assert a0 is not a1
    assert a0._environ_ is a1._environ_
    assert a0._locker_.cache is a1._locker_.cache

    assert a0 is not b and a1 is not b
    assert b._environ_ is not a0._environ_
    assert b._locker_.cache is not a0._locker_.cache

    assert c00 is not c01
    assert c00._environ_ is c01._environ_
    assert c00._locker_.cache is c01._locker_.cache

    assert c10 is not c01 and c10 is not c00
    assert c10._environ_ is c00._environ_
    assert c10._locker_.cache is not c00._locker_.cache


def test_dummy_cache(tmp_path):
    dbdict = SafeLmdbDict(tmp_path, cache=DummyCache)

    dbdict['a'] = {'value': [1]}

    assert 'a' in dbdict
    assert dbdict['a'] == {'value': [1]}

    dbdict.update([('a', {'value': [0]})], b={'value': [1]})

    assert dbdict == {'a': {'value': [0]}, 'b': {'value': [1]}}

    assert 'a' not in dbdict._locker_.cache
    assert len(dbdict._locker_.cache) == 0

    assert isinstance(dbdict._locker_.cache, DummyCache)
    assert isinstance(dbdict._locker_.locks, DummyLockPool)


def test_cache_spec_conflict(tmp_path):
    a = SafeLmdbDict(tmp_path, cache=LRUCache128)

    assert not isinstance(a._locker_.cache, DummyCache)

    with pytest.raises(TypeError):
        SafeLmdbDict(tmp_path, cache=DummyCache)
