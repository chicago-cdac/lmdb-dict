"""Manage a pool of named shared Locks."""

import threading


class NamedLock:
    """A Lock managed by a NamedLockPool.

    Note that NamedLock is *not* intended for use outside of a
    NamedLockPool. See NamedLockPool.acquire for retrieval, registration
    and acquisition of a named lock.

    NamedLock.release is this object's only public method, and expected
    to be used in conjunction with context management.

    For example:

        lock_pool = NamedLockPool()

        with lock_pool.acquire('some-name'):
            ... locked code ...

    In the above example, NamedLockPool.acquire returns a NamedLock,
    whose context management interface ensures that its release() method
    is invoked upon exiting the context.

    """
    _lock_class_ = threading.Lock

    _reentrant_ = False

    __slots__ = ('pool', 'name', 'count', '_lock', '_local')

    def __init__(self, pool, name):
        self.pool = pool
        self.name = name

        self.count = 0

        self._lock = self._lock_class_()

        self._local = threading.local()

        #
        # note: these defaults will only be set in the lucky thread that
        # instantiates the lock. others, which retrieve and share it,
        # won't see them. hence, we'll need to handle these with getattr.
        #
        self._local.acquisition_count = 0
        self._local.registration_count = 0

    def _register_(self):
        """Increment the use registration counter such that the lock is
        not prematurely discarded.

        """
        if not self._reentrant_ and getattr(self._local, 'registration_count', 0):
            raise TypeError(f'{self.__class__.__name__} may not be re-registered prior to release')

        self.count += 1
        self._local.registration_count = getattr(self._local, 'registration_count', 0) + 1

    def _acquire_(self):
        """Acquire the underlying lock -- blocking if necessary."""
        if not getattr(self._local, 'registration_count', 0):
            raise TypeError(f'{self.__class__.__name__} may not be acquired prior to registration')

        if not self._reentrant_ and getattr(self._local, 'acquisition_count', 0):
            raise TypeError(f'{self.__class__.__name__} may not be re-acquired prior to release')

        try:
            self._lock.acquire()
        except BaseException:
            # if interrupted return to consistent state
            self._unregister_()
            raise

        self._local.acquisition_count = getattr(self._local, 'acquisition_count', 0) + 1

        return True

    def _unregister_(self):
        if getattr(self._local, 'registration_count', 0):
            self.count -= 1
            self._local.registration_count -= 1

            self.pool._release_(self)

    def release(self):
        """Release the lock such that another thread may acquire it.

        Note that while this method certainly may be invoked directly it
        is best left to the context manager interface instead.

        Otherwise, its proper use may look like the following:

            lock_pool = NamedLockPool()

            named_lock = None

            try:
                named_lock = lock_pool.acquire('some-name')

                ... locked code ...
            finally:
                if named_lock:
                    named_lock.release()

        """
        if not getattr(self._local, 'acquisition_count', 0):
            raise TypeError(f'{self.__class__.__name__} may not be released prior to acquisition')

        self._unregister_()

        try:
            self._lock.release()
        except RuntimeError:
            # in event of error may not have actually locked
            pass

        self._local.acquisition_count -= 1

    def __enter__(self):
        if not self._local.acquisition_count:
            raise TypeError(f"{self.__class__.__name__} will not lock automatically, "
                            f"use {self.pool.__class__.__name__}")

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


class FullLock:
    """A global lock for a NamedLockPool.

    See: NamedLockPool.acquire_all.

    """
    __slots__ = ('pool',)

    def __init__(self, pool):
        self.pool = pool

    def _acquire_(self):
        # permit existing locks to release --
        # without attempting to acquire pool lock, which we will hold
        self.pool._fully_locked_ = True

        # prevent any new pool lock acquisitions
        self.pool._pool_lock_.acquire()

        # in event of race between two FullLocks, winner may have unset
        # _fully_locked_
        self.pool._fully_locked_ = True

        for lock in self.pool._locks_.values():
            lock._register_()
            lock._acquire_()

    def release(self):
        self.pool._fully_locked_ = False

        self.pool._pool_lock_.release()

        for lock in self.pool._locks_.values():
            lock.release()

    def __enter__(self):
        if not self.pool._fully_locked_:
            raise TypeError(f"{self.__class__.__name__} will not lock automatically, "
                            f"use {self.pool.__class__.__name__}")

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


class NamedLockPool:
    """A pool of named shared Locks.

    In lieu of direct management of either a general singular lock or
    arbitrary fine-grained locks, NamedLockPool ensures that there is
    no more than one lock for a given name at any time, available for
    threads to request as needed.

    Only once a NamedLock's registered use counter (like a reference
    count) drops to zero will it be discarded. Subsequent requests for
    the same name will return a new lock to be shared for that name.

    This functionality streamlines the management of multiple locks.
    Most important, this enables the use of an arbitrary number of
    shared locks, keyed by the shared resource they are intended to
    protect.

    Unlike the underlying threading.Lock, the NamedLock must be
    requested from a NamedLockPool. It is expected that NamedLocks are
    acquired on an as-needed basis, to ensure an accurate accounting of
    their use-count and such that they may be discarded appropriately.

    For example:

        lock_pool = NamedLockPool()

        with lock_pool.acquire('some-name'):
            ... locked code ...

    In the above example, NamedLockPool.acquire:

    * either retrieves an existing NamedLock or creates a new one;
    * increments the NamedLock's count of users;
    * acquires (waits on) the underlying lock;
    * returns the NamedLock, whose context management interface ensures
      that its release() method is invoked upon exiting the context,
      (and so also decrements the NamedLock's count of users).

    Arbitrary provisioning of locks via NamedLockPool enables even higher-
    level code, for example:

        def make_keylock(lock_pool):
            def keylock(func):
                @functools.wraps(func)
                def wrapped(key):
                    with lock_pool.acquire(key):
                        return func(key)

                return wrapped

            return keylock

        lock_pool = NamedLockPool()

        keylock = make_keylock(lock_pool)

        @keylock
        def sync_mappings_0(key):
            ...

        @keylock
        def sync_mappings_1(key):
            ...

    In the above example of two functions which may race with each
    other, rather than sharing a general-purpose Lock, they are
    decorated such that upon invocation they will acquire a NamedLock
    specific to the key of the shared mapping(s) they concern. In this
    implementation, concurrent invocations with disparate keys will not
    block.

    """
    _lock_class_ = NamedLock

    __slots__ = ('_locks_', '_pool_lock_', '_fully_locked_')

    def __init__(self):
        self._locks_ = {}
        self._pool_lock_ = threading.Lock()
        self._fully_locked_ = False

    def acquire(self, name):
        """Acquire a NamedLock shared for the given key."""
        with self._pool_lock_:
            try:
                lock = self._locks_[name]
            except KeyError:
                lock = self._locks_[name] = self._lock_class_(self, name)

            lock._register_()

        lock._acquire_()

        return lock

    def acquire_all(self):
        """Acquire a global lock for all keys.

        Acquires all extant locks and blocks creation of new locks until
        release().

        """
        lock = FullLock(self)
        lock._acquire_()
        return lock

    def _release_(self, lock):
        """Eject the released NamedLock from the pool if its
        registration count is zero.

        """
        if self._fully_locked_:
            return

        with self._pool_lock_:
            if lock.count == 0:
                del self._locks_[lock.name]


class NamedRLock(NamedLock):
    """A re-entrant Lock managed by a NamedRLockPool."""

    _lock_class_ = threading.RLock

    _reentrant_ = True

    __slots__ = ()


class NamedRLockPool(NamedLockPool):
    """A pool of named shared re-entrant Locks."""

    _lock_class_ = NamedRLock

    __slots__ = ()


class DummyLockPool(NamedLockPool):
    """NamedLockPool which never locks."""

    __slots__ = ()

    def __init__(self):
        self._pool_lock_ = None
        self._locks_ = None

    def acquire(self, name):
        """Construct a DummyLock."""
        return DummyLock(self, name)

    def acquire_all(self):
        """Construct a DummyFullLock."""
        return DummyFullLock(self)

    def _release_(self, _lock):
        """dummy: no-op"""


class DummyLock(NamedLock):
    """NamedLock which does not lock."""

    __slots__ = ()

    def __init__(self, pool, name):
        self.pool = pool
        self.name = name

        self.count = None

        self._lock = None

        self._local = None

    def _register_(self):
        """dummy: no-op"""

    def _acquire_(self):
        """dummy: no-op"""
        return True

    def _unregister_(self):
        """dummy: no-op"""

    def release(self):
        """dummy: no-op"""

    def __enter__(self):
        """dummy: no-op"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """dummy: no-op"""


class DummyFullLock(FullLock):
    """FullLock which does not lock."""

    __slots__ = ()

    def _acquire_(self):
        """dummy: no-op"""

    def release(self):
        """dummy: no-op"""

    def __enter__(self):
        """dummy: no-op"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """dummy: no-op"""
