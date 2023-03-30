import collections.abc
import contextlib
import functools
import itertools
import os
import sys
import weakref
from abc import abstractmethod
from dataclasses import dataclass

import lmdb

from lmdb_dict.cache import DummyCache
from lmdb_dict.util import (
    apply_first,
    argument_decorator,
    BytesStr,
    DummyLockPool,
    NamedLockPool,
    NamedRLockPool,
)


#
# keycompose
#
# decorator to convert first argument of wrapped callable
# (here it will be the look-up "key") to a composite of its
# decoded str and encoded bytes versions
#
# as such, LmdbDict interface keys may be either. at issue is
# that lmdb requires bytes. but we'll use str in our cache
# (and presume str will be used most often in the interface
# tho bytes fine).
#
# notably keys *may not* be other than str or bytes. (lmdb
# only supports bytes and it's up to the user to supply at
# least a str.)
#
def make_bstr(key):
    """(str or bytes) -> BytesStr

    Wraps BytesStr.make to provide use-specific error message in event
    of argument which is neither bytes nor key.

    """
    try:
        return BytesStr.make(key)
    except BytesStr.BStrTypeError as exc:
        raise TypeError(f'LmdbDict {exc.message}') from None


keycompose = argument_decorator(apply_first(make_bstr))


def keylock(method):
    """Wrap the decorated LmdbDict method to execute only having
    acquired a NamedLock for the given look-up key.

    This way, methods may narrow the scope of locking to only affect
    other operations concerning the same key.

    Presumes that:

    * the key will be the first non-instance argument, and that the
    keycompose decorator has already converted this value to a BytesStr
    * the LmdbDict's NamedLockPool is accessible at `_locker_.locks`

    """
    @functools.wraps(method)
    def wrapped(self, key, *args, **kwargs):
        if not isinstance(key, BytesStr):
            raise TypeError(f'keylock expected BytesStr not {key.__class__.__name__} '
                            f'(decorators properly ordered?)')

        with self._locker_.locks.acquire(str(key)):
            return method(self, key, *args, **kwargs)

    return wrapped


class missing:
    """Sentinel type indicating a missing value.

    Useful where None may be a user-supplied, not missing, value.

    """
    __slots__ = ()

    def __repr__(self):
        return 'missing'


_lc_dataclass = (
    dataclass(frozen=True,
              slots=True,
              weakref_slot=True)
    if sys.version_info >= (3, 11) else
    dataclass(frozen=True)
)


@_lc_dataclass
class LockingCache:
    """Composition of a cache with its locks.

    LockingCache ensures associated caches and locks remain associated
    when in use across LmdbDict instances.

    """
    cache: collections.abc.MutableMapping
    locks: NamedRLockPool


class LmdbDict(collections.abc.MutableMapping):
    """Abstract base class providing a dictionary interface to LMDB.

    LMDB stores keys and values as bytes. Keys are encoded/decoded
    from/to str by this base class. Concrete subclasses are expected to
    define values' (de)serialization methods:

    * _serialize_
    * _deserialize_

    Only databases configured for non-duplicative keys are supported.

    """
    #
    # internal sentinel (which we know is not a user value)
    # used to indicate a missing value
    #
    # private to mirror (and respect) MutableMapping
    #
    __marker = missing()

    #
    # dangerous to open multiple lmdb.Environment per path and process
    #
    _environs_ = weakref.WeakValueDictionary()

    #
    # (and, share the cache, and make it authoritative)
    #
    _lockers_ = weakref.WeakValueDictionary()

    #
    # (and protect these class-level dicts as well)
    #
    _init_locks_ = NamedLockPool()

    __slots__ = ('_environ_', '_db_', '_locker_', 'name')

    def __init__(self, path, name=None, *, cache=DummyCache, **lmdb_open):
        with self._init_locks_.acquire(str(path)):
            try:
                self._environ_ = self._environs_[str(path)]
            except KeyError:
                self._environ_ = self._environs_[str(path)] = lmdb.open(str(path), **lmdb_open)

        self._db_ = None if name is None else self._environ_.open_db(name.encode())

        self.name = name

        #
        # note: cache is *not* intended to cache values *generally* from db
        #
        # (db is expected to be memory-mapped and pretty darn fast as it is)
        #
        # rather, cache is intended for *deserialized* objects, to save cycles
        # repeatedly processing these.
        #
        cache_key = self.full_path(strip=False)

        with self._init_locks_.acquire(cache_key):
            try:
                self._locker_ = self._lockers_[cache_key]
            except KeyError:
                locker_cache = cache() if callable(cache) else cache

                locker_locks = (DummyLockPool() if getattr(locker_cache, 'maxsize', None) == 0
                                else NamedRLockPool())

                self._locker_ = self._lockers_[cache_key] = LockingCache(locker_cache,
                                                                         locker_locks)
            else:
                if (
                    # check whether they got exactly what they wanted...
                    cache is not self._locker_.cache
                    # ...or at least an instance of the class they wanted
                    and (not isinstance(cache, type) or type(self._locker_.cache) is not cache)
                ):
                    raise TypeError(f'requested cache {cache!r} but {self._locker_.cache!r} '
                                    f'already in use for {cache_key!r}')

    @property
    def path(self):
        return self._environ_.path()

    def full_path(self, *, strip=True):
        if self.name:
            return f'{self.path}{os.pathsep}{self.name}'

        if not strip:
            return f'{self.path}{os.pathsep}'

        return self.path

    def __repr__(self):
        return f'<{self.__class__.__name__} for {self.full_path()}>'

    #
    # abstract methods for subclasses
    #

    @staticmethod
    @abstractmethod
    def _serialize_(value):
        pass

    @staticmethod
    @abstractmethod
    def _deserialize_(raw):
        pass

    #
    # concrete methods for collections.abc
    #

    @keycompose
    @keylock
    def __getitem__(self, key):
        """Get self[key] from the database."""
        try:
            value = self._locker_.cache[str(key)]
        except KeyError:
            pass
        else:
            if value is self.__marker:
                raise KeyError(str(key))

            return value

        with self._environ_.begin(db=self._db_, buffers=True) as txn:
            buffer = txn.get(bytes(key))

            if buffer is None:
                self._locker_.cache[str(key)] = self.__marker
                raise KeyError(str(key))

            value = self._deserialize_(buffer)

        self._locker_.cache[str(key)] = value

        return value

    @keycompose
    @keylock
    def __setitem__(self, key, value):
        """Set self[key] to value in the database."""
        encoded = self._serialize_(value)

        with self._environ_.begin(db=self._db_, write=True) as txn:
            txn.put(bytes(key), encoded)

        self._locker_.cache[str(key)] = value

    @keycompose
    @keylock
    def __delitem__(self, key):
        """Delete self[key] from the database."""
        with self._environ_.begin(db=self._db_, write=True) as txn:
            deleted = txn.delete(bytes(key))

        self._locker_.cache[str(key)] = self.__marker

        if not deleted:
            raise KeyError(str(key))

    def __len__(self):
        """Retrieve len(self) from the database."""
        if self._db_:
            with self._environ_.begin(db=self._db_) as txn:
                stats = txn.stat()
        else:
            stats = self._environ_.stat()

        return stats['entries']

    def __iter__(self):
        """Return the forward iterator iter(self) of database keys."""
        for key in self._iter_(keys=True, values=False):
            yield str(key, encoding=BytesStr.encoding)

    #
    # extending collections.abc
    #

    def __reversed__(self):
        """Return the reverse iterator reversed(self) of database keys."""
        for key in self._iter_(keys=True, values=False, forward=False):
            yield str(key, encoding=BytesStr.encoding)

    #
    # optimizations
    #

    # internal

    def _iter_(self, keys=True, values=True, forward=True):
        with self._environ_.begin(db=self._db_, buffers=True) as txn:
            with txn.cursor() as cursor:
                method = cursor.iternext if forward else cursor.iterprev
                yield from method(keys=keys, values=values)

    def _iteritems_(self):
        #
        # lmdb provides a "stable" view of the database, versioned from the time
        # the transaction is initiated.
        #
        # that's nice; and, though our cache doesn't work that way, we *could*
        # just *ignore* the cache.
        #
        # (and, with a dummy cache, that will be the functionality.)
        #
        # however, so long as we are caching, it would be nice to use it here as
        # well.
        #
        # the lmdb api does not provide object version information.
        #
        # rather, so long as we're caching, we *must* check cache as below,
        # (and so, as implemented simply override the versioned view):
        #
        # * if a competing thread has changed the cache -- and presumably the db
        #   -- then we don't want to potentially spoil the cache here with stale
        #   deserialized data (in the "except" block)
        #
        #   rather than attempt to discern versioning, if anything's in the cache,
        #   we'll use it, in place of what we received.
        #
        #   * if it's newer, great! the cache helped us ensure the *latest* data
        #     is returned (without spawning a transaction for each key); and, we
        #     didn't attempt to spoil the cache
        #
        #   * if it's the same, great! as ever, the cache helped us avoid re-
        #     deserialization
        #
        # * should we beat a competing thread in changing the cache -- fine! --
        #   that's how it works
        #
        #   then we're still operating correctly -- caching deserialized db
        #   values -- and the cache will be updated by the competing thread once
        #   it gets around to it.
        #
        #   (obviously, this is safe only thanks to the key lock.)
        #
        # all that said, it ONLY holds IFF the cache is large enough to hold all
        # values. if it's not ... there's really no way to know that our view's
        # data is the latest.
        #
        # in that case, we can retrieve from the cache, but *never* update it.
        #
        # and, in that case, this method may generate a mix of stale and latest
        # values! this is now presumed to be fine; (but, could be adapted to
        # handle that case instead by *only* generating potentially stale values).
        #
        update_cache = self._locker_.cache.maxsize >= len(self)

        for (key_r, value_r) in self._iter_():
            key = str(key_r, encoding=BytesStr.encoding)

            # keylock required IFF we're updating the cache
            manager = (self._locker_.locks.acquire(key) if update_cache
                       else contextlib.nullcontext())

            with manager:
                try:
                    value = self._locker_.cache[key]
                except KeyError:
                    value = self._deserialize_(value_r)

                    if update_cache:
                        self._locker_.cache[key] = value
                else:
                    if value is self.__marker:
                        #
                        # item was deleted since we began iteration
                        #
                        # we'll defer to latest:
                        #
                        # * despite "inconsistency" of not reflecting our
                        #   transaction's view
                        #
                        # * really to be consistent with necessary use of
                        #   cache above
                        #
                        continue

            yield (key, value)

    # external

    @keycompose
    def __contains__(self, key):
        try:
            value = self._locker_.cache[str(key)]
        except KeyError:
            pass
        else:
            return value is not self.__marker

        with self._environ_.begin(db=self._db_) as txn:
            with txn.cursor() as cursor:
                return cursor.set_key(bytes(key))

    def items(self):
        "D.items() -> a set-like object providing a view on D's items"
        return LmdbItemsView(self)

    def values(self):
        "D.values() -> an object providing a view on D's values"
        return LmdbValuesView(self)

    @keycompose
    @keylock
    def pop(self, key, default=__marker):
        """D.pop(k[,d]) -> v: remove specified key and return the
        corresponding value.

        If key is not found, default is returned if given, otherwise
        KeyError is raised.

        """
        try:
            value = self._locker_.cache[str(key)]
        except KeyError:
            pass
        else:
            if value is self.__marker:
                if default is self.__marker:
                    raise KeyError(str(key))

                return default

            del self[key]
            return value

        with self._environ_.begin(db=self._db_, buffers=True, write=True) as txn:
            value_r = txn.pop(bytes(key))

            value = self.__marker if value_r is None else self._deserialize_(value_r)

        self._locker_.cache[str(key)] = self.__marker

        if value is self.__marker:
            if default is self.__marker:
                raise KeyError(str(key))

            return default

        return value

    def popitem(self, *, last=True):
        """D.popitem() -> (k, v): remove and return last or first
        (key, value) pair as a 2-tuple.

        LMDB -- and, by extension, LmdbDict -- differ from the Python
        dict in that they are not insertion-ordered. Rather, keys are
        alphanumerically sorted.

        As such, this method mirrors the Python dict version only in so
        far as pairs are removed and returned in reverse alphanumeric
        order -- comparable to but different from LIFO (last-in
        first-out) order.

        Moreover, pairs may be removed and returned in forward
        alphanumeric order, given keyword argument `last=False`.

        Raises KeyError if the dict is empty.

        """
        with self._environ_.begin(db=self._db_, buffers=True, write=True) as txn:
            #
            # any subsequent write transaction on this environ is now blocked
            # until this transaction is closed.
            #
            # as such, this isn't merely a view: the db state is stable.
            #
            with txn.cursor() as cursor:
                found = cursor.last() if last else cursor.first()

                if not found:
                    # db is empty
                    raise KeyError

                (key_r, value_r) = cursor.item()

                key = str(key_r, encoding=BytesStr.encoding)

                with self._locker_.locks.acquire(key):
                    #
                    # and now all other methods are blocked from modifying
                    # this key in the cache, as well.
                    #
                    # we're safe to modify both.
                    #
                    try:
                        value = self._locker_.cache[key]
                    except KeyError:
                        value = self._deserialize_(value_r)

                    cursor.delete()

                    self._locker_.cache[key] = self.__marker

        return (key, value)

    def clear(self):
        """D.clear() -> None: remove all items from D."""
        with self._environ_.begin(write=True) as txn:
            #
            # any subsequent write transaction on this environ is now blocked
            # until this transaction is closed.
            #
            # as such, this isn't merely a view: the db state is stable.
            #
            if self._db_:
                txn.drop(self._db_, delete=False)
            else:
                with txn.cursor() as cursor:
                    if cursor.first():
                        while cursor.delete():
                            pass

            #
            # as with other multi-key methods (e.g. _iteritems_), we must now
            # find a means to protect against another method's racing with this
            # one, e.g.:
            #
            # 1. __getitem__ begins, retrieving an uncached value from the db
            # 2. before completion of the above, this method clears db and cache
            # 3. following completion of this method, __getitem__ resumes, and
            #    caches its now-stale value before returning
            #
            # *unlike* with such multi-key, iterative methods, this method is
            # interface-atomic -- we may safely lock the *entire* cache across
            # all keys.
            #
            # with the below global lock, we are ensured that any extant cache
            # transactions have completed, and that no new ones will be started.
            #
            # __getitem__ -- stale or not -- must complete first, and then be
            # overwritten.
            #
            with self._locker_.locks.acquire_all():
                for key in self._locker_.cache:
                    self._locker_.cache[key] = self.__marker

    def update(self, other=(), /, **kwargs):
        """D.update([E, ]**F) -> None: Update D from mapping/iterable E and F.

        If E present and has a .keys() method, does: for k in E: D[k] = E[k]
        If E present and lacks .keys() method, does: for (k, v) in E: D[k] = v

        In either case, this is followed by: for k, v in F.items(): D[k] = v

        """
        if isinstance(other, collections.abc.Mapping):
            items0 = other.items()
        elif hasattr(other, 'keys'):
            items0 = ((key, other[key]) for key in other.keys())
        else:
            items0 = other

        items1 = kwargs.items()

        items = [
            (BytesStr.make(key), value)
            for (key, value) in itertools.chain(items0, items1)
        ]

        with self._locker_.locks.acquire_all():
            with self._environ_.begin(db=self._db_, write=True) as txn:
                with txn.cursor() as cursor:
                    cursor.putmulti(
                        (bytes(key), self._serialize_(value))
                        for (key, value) in items
                    )

            self._locker_.cache.update((str(key), value) for (key, value) in items)

    @keycompose
    @keylock
    def setdefault(self, key, default=None):
        """D.setdefault(k[,d]) -> D.get(k,d), also set D[k]=d if k not in D"""
        try:
            value = self._locker_.cache[str(key)]
        except KeyError:
            pass
        else:
            if value is not self.__marker:
                return value

        with self._environ_.begin(db=self._db_, buffers=True, write=True) as txn:
            with txn.cursor() as cursor:
                if cursor.set_key(bytes(key)):
                    buffer = cursor.value()
                    value = self._deserialize_(buffer)
                else:
                    cursor.put(bytes(key), self._serialize_(default))
                    value = default

        self._locker_.cache[str(key)] = value

        return value


class LmdbItemsView(collections.abc.ItemsView):

    __slots__ = ()

    # optimization

    def __iter__(self):
        yield from self._mapping._iteritems_()


class LmdbValuesView(collections.abc.ValuesView):

    __slots__ = ()

    # optimization

    def __contains__(self, value):
        for v in self:
            if v is value or v == value:
                return True

        return False

    def __iter__(self):
        for item in self._mapping._iteritems_():
            yield item[1]
