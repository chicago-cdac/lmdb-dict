import re
import zlib

import lmdb
import pytest
import yaml

from lmdb_dict import SafeLmdbDict, StrLmdbDict, CachedLmdbDict
from lmdb_dict.cache import DummyCache, LRUCache128
from lmdb_dict.util import DummyLockPool, NamedRLockPool


KEY = 'aaa'

VALUE = {'value': ['super-califragilistic expialidocious!?']}


def prep_db(path, name=None, max_dbs=0):
    with lmdb.open(str(path), max_dbs=max_dbs) as env:
        db = None if name is None else env.open_db(name.encode())

        with env.begin(db=db, write=True) as txn:
            txn.put(
                KEY.encode(),
                zlib.compress(yaml.safe_dump(VALUE, encoding='utf-8'))
            )


@pytest.fixture
def dbdict(tmp_path):
    prep_db(tmp_path)

    return SafeLmdbDict(tmp_path, cache=LRUCache128)


@pytest.fixture
def nameddict(tmp_path):
    prep_db(tmp_path, 'alt', 2)

    return SafeLmdbDict(tmp_path, 'alt', cache=LRUCache128, max_dbs=2)


class BadSerializer(SafeLmdbDict):

    __slots__ = ('serialize_results',)

    defer = object()

    def __init__(self, *args, serialize_results=(), **kwargs):
        super().__init__(*args, **kwargs)
        self.serialize_results = iter(serialize_results)

    def _serialize_(self, value):
        result = next(self.serialize_results)

        if result is self.defer:
            return super()._serialize_(value)

        if isinstance(result, Exception) or (isinstance(result, type) and
                                             issubclass(result, Exception)):
            raise result

        return result


def test_getitem(dbdict):
    assert not dbdict._locker_.cache

    assert dbdict[KEY] == VALUE
    assert dbdict[KEY.encode()] == VALUE

    assert dbdict._locker_.cache

    dbdict._environ_ = None

    assert dbdict[KEY] == VALUE
    assert dbdict[KEY.encode()] == VALUE

    with pytest.raises(TypeError):
        dbdict[None]

    assert SafeLmdbDict.__getitem__(dbdict, KEY) == VALUE


def test_setitem(dbdict):
    pyvalue = {'value': 'yet another 😉'}

    assert not dbdict._locker_.cache

    dbdict['bbb'] = pyvalue

    assert len(dbdict._locker_.cache) == 1

    with dbdict._environ_.begin() as txn:
        dbvalue = txn.get(b'bbb')

    assert yaml.safe_load(zlib.decompress(dbvalue)) == pyvalue

    client = dbdict._environ_
    dbdict._environ_ = None

    assert dbdict['bbb'] == pyvalue

    dbdict._environ_ = client
    dbdict._locker_.cache.clear()

    assert dbdict['bbb'] == pyvalue

    assert len(dbdict._locker_.cache) == 1

    assert dbdict[KEY] == VALUE

    assert len(dbdict._locker_.cache) == 2


def test_delitem(dbdict):
    assert not dbdict._locker_.cache

    del dbdict[KEY]

    assert len(dbdict._locker_.cache) == 1

    with dbdict._environ_.begin() as txn:
        assert txn.get(KEY.encode()) is None

    client = dbdict._environ_
    dbdict._environ_ = None

    with pytest.raises(KeyError):
        dbdict[KEY]

    dbdict._environ_ = client
    dbdict._locker_.cache.clear()

    with pytest.raises(KeyError):
        dbdict[KEY]

    assert len(dbdict._locker_.cache) == 1

    with pytest.raises(KeyError):
        del dbdict['bbb']

    assert len(dbdict._locker_.cache) == 2


def test_iter(dbdict):
    assert not dbdict._locker_.cache

    dbdict['bbb'] = [0, 1]

    assert len(dbdict._locker_.cache) == 1

    assert list(dbdict) == [KEY, 'bbb']

    assert len(dbdict._locker_.cache) == 1


def test_reversed(dbdict):
    assert not dbdict._locker_.cache

    dbdict['bbb'] = [0, 1]

    assert len(dbdict._locker_.cache) == 1

    assert list(reversed(dbdict)) == ['bbb', KEY]

    assert len(dbdict._locker_.cache) == 1


def test_len(dbdict):
    assert not dbdict._locker_.cache

    assert len(dbdict) == 1

    assert not dbdict._locker_.cache


def test_contains(dbdict):
    assert not dbdict._locker_.cache

    assert KEY in dbdict

    assert not dbdict._locker_.cache

    assert 'bbb' not in dbdict

    assert not dbdict._locker_.cache

    assert dbdict[KEY] == VALUE

    with pytest.raises(KeyError):
        dbdict['bbb']

    assert len(dbdict._locker_.cache) == 2

    dbdict._environ_ = None

    assert KEY in dbdict

    assert 'bbb' not in dbdict


def test_keys(dbdict):
    keys = dbdict.keys()

    assert len(keys) == 1

    assert KEY in keys

    assert 'bbb' not in keys

    assert list(keys) == [KEY]

    assert not dbdict._locker_.cache


def test_items(dbdict):
    items = dbdict.items()

    assert len(items) == 1

    assert not dbdict._locker_.cache

    assert (KEY, VALUE) in items

    assert len(dbdict._locker_.cache) == 1

    assert (KEY, 'hi') not in items

    assert len(dbdict._locker_.cache) == 1

    assert ('bye', 'hi') not in items

    assert len(dbdict._locker_.cache) == 2

    dbdict._locker_.cache.clear()

    assert list(items) == [(KEY, VALUE)]

    assert len(dbdict._locker_.cache) == 1


def test_values(dbdict):
    values = dbdict.values()

    assert len(values) == 1

    assert not dbdict._locker_.cache

    assert VALUE in values

    assert len(dbdict._locker_.cache) == 1

    assert 'bbb' not in values

    assert len(dbdict._locker_.cache) == 1

    dbdict._locker_.cache.clear()

    assert list(values) == [VALUE]

    assert len(dbdict._locker_.cache) == 1


def test_get(dbdict):
    assert not dbdict._locker_.cache

    assert dbdict.get(KEY) == VALUE

    assert len(dbdict._locker_.cache) == 1

    assert dbdict.get('bbb') is None

    assert dbdict.get('bbb', 'default') == 'default'

    assert len(dbdict._locker_.cache) == 2

    assert dbdict._environ_.stat()['entries'] == 1


def test_pop(dbdict):
    pyvalue = {'value': 'yet another 😉'}

    assert not dbdict._locker_.cache

    dbdict['bbb'] = pyvalue

    assert list(dbdict._locker_.cache.items()) == [('bbb', pyvalue)]

    assert dbdict._environ_.stat()['entries'] == 2

    assert dbdict.pop('bbb') == pyvalue

    assert dbdict._environ_.stat()['entries'] == 1

    assert list(dbdict._locker_.cache.items()) == [('bbb', dbdict._LmdbDict__marker)]

    assert dbdict.pop(KEY) == VALUE

    assert dbdict._environ_.stat()['entries'] == 0

    assert len(dbdict._locker_.cache) == 2

    assert dbdict._locker_.cache[KEY] is dbdict._LmdbDict__marker

    # grab that line as well
    assert repr(dbdict._locker_.cache[KEY]) == 'missing'

    client = dbdict._environ_
    dbdict._environ_ = None

    with pytest.raises(KeyError):
        dbdict.pop(KEY)

    assert dbdict.pop(KEY, 'default') == 'default'

    dbdict._environ_ = client
    dbdict._locker_.cache.clear()

    assert dbdict.pop(KEY, 'default') == 'default'

    assert len(dbdict._locker_.cache) == 1

    with pytest.raises(KeyError):
        dbdict.pop('bbb')

    assert len(dbdict._locker_.cache) == 2


def test_popitem(dbdict):
    pyvalue0 = {'value': 'yet another 😉'}
    pyvalue1 = {'wait': ["there's more?"]}

    assert not dbdict._locker_.cache

    dbdict['bbb'] = pyvalue0
    dbdict['ccc'] = pyvalue1

    assert len(dbdict) == 3
    assert len(dbdict._locker_.cache) == 2

    assert dbdict.popitem() == ('ccc', pyvalue1)

    assert len(dbdict) == 2
    assert len(dbdict._locker_.cache) == 2

    assert dbdict.popitem(last=False) == (KEY, VALUE)

    assert len(dbdict) == 1
    assert len(dbdict._locker_.cache) == 3

    dbdict.popitem()

    with pytest.raises(KeyError):
        dbdict.popitem()

    assert len(dbdict) == 0

    assert dbdict._locker_.cache == {KEY: dbdict._LmdbDict__marker,
                                     'bbb': dbdict._LmdbDict__marker,
                                     'ccc': dbdict._LmdbDict__marker}


def test_clear_default(dbdict):
    assert len(dbdict) == 1
    assert not dbdict._locker_.cache

    assert dbdict[KEY] == VALUE

    assert len(dbdict._locker_.cache) == 1

    dbdict.clear()

    assert len(dbdict) == 0
    assert dbdict._locker_.cache == {KEY: dbdict._LmdbDict__marker}

    # should work no less
    dbdict.clear()

    assert len(dbdict) == 0
    assert dbdict._locker_.cache == {KEY: dbdict._LmdbDict__marker}


def test_clear_named(nameddict):
    assert len(nameddict) == 1
    assert not nameddict._locker_.cache

    assert nameddict[KEY] == VALUE

    assert len(nameddict._locker_.cache) == 1

    nameddict.clear()

    assert len(nameddict) == 0
    assert nameddict._locker_.cache == {KEY: nameddict._LmdbDict__marker}


def test_update(dbdict):
    pyvalue0 = {'value': 'yet another 😉'}
    pyvalue1 = {'wait': ["there's more?"]}

    assert len(dbdict) == 1
    assert not dbdict._locker_.cache

    dbdict.update([('bbb', pyvalue0)], ccc=pyvalue1)

    assert len(dbdict) == 3
    assert len(dbdict._locker_.cache) == 2

    dbdict.update(pyvalue1)

    assert len(dbdict) == 4
    assert len(dbdict._locker_.cache) == 3

    assert dbdict['wait'] == pyvalue1['wait']
    assert dbdict['ccc'] == pyvalue1
    assert dbdict['bbb'] == pyvalue0
    assert dbdict[KEY] == VALUE

    dbdict._locker_.cache.clear()

    assert dbdict == {
        KEY: VALUE,
        'bbb': pyvalue0,
        'ccc': pyvalue1,
        'wait': pyvalue1['wait'],
    }


def test_update_error(tmp_path):
    dbdict = BadSerializer(tmp_path,
                           cache=LRUCache128,
                           serialize_results=(BadSerializer.defer, ValueError))

    with pytest.raises(ValueError):
        dbdict.update(a={'value': 0}, b={'value': 1})

    assert len(dbdict) == 0
    assert len(dbdict._locker_.cache) == 0


def test_setdefault(dbdict):
    pyvalue0 = {'value': 'yet another 😉'}

    assert not dbdict._locker_.cache

    assert dbdict.setdefault(KEY, 'boo') == VALUE

    assert dbdict._locker_.cache[KEY] == VALUE

    assert dbdict.setdefault('bbb', pyvalue0) == pyvalue0

    assert len(dbdict) == 2

    assert dbdict._locker_.cache['bbb'] == pyvalue0

    client = dbdict._environ_
    dbdict._environ_ = None

    assert dbdict.setdefault(KEY, 'boo') == VALUE

    dbdict._environ_ = client

    del dbdict[KEY]

    assert dbdict.setdefault(KEY, 'boo') == 'boo'

    assert len(dbdict) == 2

    assert dbdict._locker_.cache[KEY] == 'boo'


def test_eq(dbdict):
    assert not dbdict._locker_.cache

    assert dbdict == {KEY: VALUE}

    assert dbdict != {'bbb': 'value'}

    assert len(dbdict._locker_.cache) == 1


def test_repr_default(dbdict):
    assert re.fullmatch(r'<SafeLmdbDict for .+/test_repr_default\d+>', repr(dbdict))


def test_repr_named(nameddict):
    assert re.fullmatch(r'<SafeLmdbDict for .+/test_repr_named\d+:alt>', repr(nameddict))


def test_str_dict(tmp_path):
    with pytest.raises(TypeError):
        StrLmdbDict(tmp_path, cache=LRUCache128)

    dbdict = StrLmdbDict(tmp_path)

    assert isinstance(dbdict._locker_.cache, DummyCache)
    assert isinstance(dbdict._locker_.locks, DummyLockPool)

    assert dbdict == {}

    with pytest.raises(TypeError):
        dbdict['a'] = None

    dbdict['a'] = 'hello str'
    dbdict[b'b'] = b'hello bytes'

    assert dbdict == {'a': 'hello str', 'b': 'hello bytes'}

    assert len(dbdict) == 2

    assert len(dbdict._locker_.cache) == 0


def test_cached_dict(tmp_path):
    with pytest.raises(TypeError):
        CachedLmdbDict(tmp_path, cache=DummyCache)

    dbdict = CachedLmdbDict(tmp_path)

    assert isinstance(dbdict._locker_.cache, LRUCache128)
    assert isinstance(dbdict._locker_.locks, NamedRLockPool)

    dbdict['a'] = {'value': 1}

    assert len(dbdict) == 1
    assert len(dbdict._locker_.cache) == 1
