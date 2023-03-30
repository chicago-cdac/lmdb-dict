import logging
from threading import Thread

import pytest

from lmdb_dict import SafeLmdbDict
from lmdb_dict.cache import LRUCache, LRUCache128

from .util.tdb import ThreadDebugger


tdb = ThreadDebugger()


@pytest.fixture
def log(request):
    return logging.getLogger(request.node.name)


class BadDeserializer(SafeLmdbDict):

    __slots__ = ()

    @classmethod
    def _deserialize_(cls, raw):
        tdb.set_pause('_deserialize_')
        return super()._deserialize_(raw)


def test_get_del(log, tmp_path):
    #
    # set up dict
    #
    (KEY, VALUE) = ('aaa', {'value': 'I am data'})

    dbdict = BadDeserializer(tmp_path, cache=LRUCache128)

    dbdict[KEY] = VALUE

    assert dbdict == {KEY: VALUE}

    # pretend this is a fresh instance with an unpopulated cache
    dbdict._locker_.cache.clear()

    #
    # set up threads
    #
    getitem_result = None

    def getitem(key):
        nonlocal getitem_result
        getitem_result = dbdict[key]
        log.debug('__getitem__ retrieved: %r', getitem_result)

    thread_get = Thread(target=getitem, args=(KEY,))

    thread_del = Thread(target=dbdict.__delitem__, args=(KEY,))

    #
    # orchestrate
    #

    # getitem starts but loses cpu (forcefully via set_pause)
    thread_get.start()

    # let getitem get to its pause
    assert thread_get.is_alive()

    pause_get = tdb.get_pause('_deserialize_')
    pause_get.await_waiting()

    # delitem swoops in
    thread_del.start()

    # we want delitem to be blocked ...
    # for now ... we'll just let it stew momentarily
    thread_del.join(timeout=0.1)

    if not thread_del.is_alive():
        # whoops
        log.error('no locking')

    # getitem resumes
    pause_get.clear()

    # both complete
    thread_get.join(timeout=1)
    thread_del.join(timeout=1)

    assert not thread_get.is_alive()
    assert not thread_del.is_alive()

    #
    # validate
    #

    # db emptied by delitem
    assert len(dbdict) == 0

    # getitem started first and should return what it saw
    assert getitem_result == VALUE

    # but the cache should be consistent with the db
    with pytest.raises(KeyError):
        dbdict[KEY]

    # cache shouldn't be *empty* but should reflect emptiness
    assert len(dbdict._locker_.cache) == 1
    assert repr(dbdict._locker_.cache[KEY]) == 'missing'


def test_iter_items_slow_small(tmp_path):
    #
    # set up dict
    #
    values = {
        'aaa': [0, 1],
        'bbb': {'value': 0},
    }

    dbdict = SafeLmdbDict(tmp_path, cache=LRUCache128)

    dbdict.update(values)

    assert dbdict == values

    #
    # orchestrate
    #

    # *start* iteration
    (value0, value1) = values.items()

    iterator = iter(dbdict.items())

    assert next(iterator) == value0

    # collide with an update
    update1 = {'value': 1}

    dbdict['bbb'] = update1

    # continue iteration
    (iter1,) = iterator

    #
    # validate
    #

    # iterator reflects update (not stale value)
    assert iter1 != value1
    assert iter1 == ('bbb', update1)

    # updated value persists
    assert dbdict['bbb'] == update1


def test_iter_items_fast_small(log, tmp_path):
    #
    # set up dict
    #
    (KEY, VALUE) = ('aaa', {'value': 'I am data'})

    dbdict = BadDeserializer(tmp_path, cache=LRUCache128)

    dbdict[KEY] = VALUE

    assert dbdict == {KEY: VALUE}

    # pretend this is a fresh instance with an unpopulated cache
    dbdict._locker_.cache.clear()

    #
    # set up threads
    #
    value1 = {'value': 'I am BETTER data'}

    getitems_result = None

    def getitems():
        nonlocal getitems_result
        getitems_result = list(dbdict.items())
        log.debug('items retrieved: %r', getitems_result)

    thread_get = Thread(target=getitems)

    thread_set = Thread(target=dbdict.__setitem__, args=(KEY, value1))

    #
    # orchestrate
    #

    # getitems starts but loses cpu (forcefully via set_pause)
    thread_get.start()

    # let getitem get to its pause
    assert thread_get.is_alive()

    pause_get = tdb.get_pause('_deserialize_')
    pause_get.await_waiting()

    # setitem swoops in
    thread_set.start()

    # we want setitem to be blocked ...
    # for now ... we'll just let it stew momentarily
    thread_set.join(timeout=0.1)

    if not thread_set.is_alive():
        # whoops
        log.error('no locking')

    # getitem resumes
    pause_get.clear()

    # both complete
    thread_get.join(timeout=1)
    thread_set.join(timeout=1)

    assert not thread_get.is_alive()
    assert not thread_set.is_alive()

    #
    # validate
    #

    # iterator reflects stale value
    assert getitems_result == [(KEY, VALUE)]

    # but the updated value persists
    assert dbdict[KEY] == value1, 'cache spoiled'


def test_iter_items_slow_large(tmp_path):
    #
    # set up dict with relatively small cache
    #
    values = {
        'aaa': [0, 1],
        'bbb': {'value': 0},
        'ccc': 11,
    }

    dbdict = SafeLmdbDict(tmp_path, cache=LRUCache(maxsize=1))

    dbdict.update(values)

    assert dbdict == values

    #
    # orchestrate
    #

    # *start* iteration
    (item0, item1, item2) = values.items()

    iterator = iter(dbdict.items())

    assert next(iterator) == item0

    # collide with an update
    (update0, update1) = ({'value': 1}, 22)

    dbdict['bbb'] = update0
    dbdict['ccc'] = update1

    # continue iteration
    (iter1, iter2) = iterator

    #
    # validate
    #

    # most important: updated values persist in cache...
    assert dbdict['ccc'] == update1

    # ...and in db
    dbdict._locker_.cache.clear()

    assert dbdict['aaa'] == values['aaa']
    assert dbdict['bbb'] == update0
    assert dbdict['ccc'] == update1

    # meanwhile: iterator reflects stale values where they were missing from cache...
    assert iter1 == item1

    # ...and updated values which the cache could provide
    assert iter2 == ('ccc', update1)
