# lmdb-dict

[![PyPI - Version](https://img.shields.io/pypi/v/lmdb-dict.svg)](https://pypi.org/project/lmdb-dict)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/lmdb-dict.svg)](https://pypi.org/project/lmdb-dict)

**The full-featured `dict` interface to the LMDB "Lightning" Database.**

* Internally optimized via `lmdb` library cursors. Optional LRU caching of deserialized values. Thread-safe operations. No added reserved keys, *etc.*

* Provides value-serializing `SafeLmdbDict` and str-only `StrLmdbDict`, as well as abstract base class `LmdbDict` for customization of database encoding.

* Unique-key, labeled and unlabeled databases and read-write sessions supported.

-----

**Table of Contents**

- [Installation](#installation)
- [Use](#use)
  - [General use](#general-use)
  - [Caching](#caching)
  - [Str-only](#str-only)
- [License](#license)

## Installation

```console
pip install lmdb-dict
```

## Use

### General use

`SafeLmdbDict` provides the full `dict` interface to a LMDB database at a given filesystem path. (An empty database is automatically provisioned within a directory without one.)

Values are automatically serialized (deserialized) and compressed (decompressed) using [PyYAML](https://pypi.org/project/PyYAML/) and [zlib](https://docs.python.org/3/library/zlib.html).

```python
from lmdb_dict import SafeLmdbDict

dbdict = SafeLmdbDict('/path/to/db/directory/0/')

dbdict['aaa'] = {'values': [0, 1, 'x']}
```

One or more named databases are also supported.

LMDB requires that the maximum number of named databases is specified up-front. Below we'll only need two named databases.

```python
users = SafeLmdbDict('/path/to/db/directory/1/', name='users', max_dbs=2)

hats = SafeLmdbDict('/path/to/db/directory/1/', name='hats', max_dbs=2)
```

Note that it would otherwise be unsafe to hold open multiple `lmdb` client objects within a single process at once. This is handled automatically: a weak reference is kept to the client opened for each filesystem path and reused for each `LmdbDict` requiring it.

### Caching

Caching of LMDB itself *should not be necessary*. The database "fully exploits the operating system’s buffer cache" and memory mapping [[ref]](https://lmdb.readthedocs.io/en/release/).

Moreover, `lmdb-dict` makes every effort to use `lmdb` efficiently, such that the user need not be concerned with undue overhead of interacting with the database-backed dictionary.

That said: the value serialization layer of `SafeLmdbDict` is another matter. Given sufficiently hefty values to deserialize, it *may* be worthwhile to engage the `lmdb-dict` caching layer, along with the trade-offs that it entails.

#### Caveats

**`lmdb-dict` caching is thread-safe**

This is achieved with behind-the-scenes locking – narrowly applied to singular keys where feasible – but the small overhead of which applies when caching.

**`lmdb-dict` caching is *not* (yet) *automatically* process-safe**

Caching is thread-safe thanks to thread locks and (again) weak references to caches which must be shared across dictionaries backed by the same databases.

Achieving the same under a multiprocessing regime would be another matter.

Users may nonetheless make use of `lmdb-dict` while multiprocessing, either without caching or with thoughtful application of caches across processes.

#### Options

Caching is built into all concrete subclasses of `LmdbDict`; however, it is disabled by default, in that it is set to `DummyCache` – a mapping capable of storing zero items.

Subclasses of `LmdbDict` check their cache for its maximum capacity by means of: `getattr(cache, 'maxsize', …)`. A cache reporting `maxsize=0` – such as the `DummyCache` – will be given *dummy locks*, such that locking is disabled for this dictionary.

A cache reporting any other `maxsize` – or lacking this property – is treated as a proper cache, and locking will be applied.

Caching may be specified – to `SafeLmdbDict` for example – via an instance, a class, or any callable returning an instance of a mapping for use as a deserialization cache. Either an instance or a class are strongly recommended, as these enable checking any cache retrieved from the weak reference registry against the user's instantiation argument.

```python
from lmdb_dict.cache import LRUCache128

SafeLmdbDict('/path/to/db/directory/', cache=LRUCache128)
```

Above, we've specified that our `SafeLmdbDict` should cache deserialized values using an instance of `LRUCache128` – that is, a subclass of the `LRUCache` provided by [cachetools](https://pypi.org/project/cachetools/). `LRUCache128` distinguishes itself only in that it requires no initialization arguments – a requirement of supplying a callable in lieu of a cache instance – and it sets `maxsize=128`.

As a shortcut to the above, `lmdb-dict` provides `CachedLmdbDict`:

```python
from lmdb_dict import CachedLmdbDict

CachedLmdbDict('/path/to/db/directory/')
```

`CachedLmdbDict` differs from other subclasses of `LmdbDict` in that it defaults to caching via `LRUCache128`. Other caches may be specified via the `cache` argument. Supplying an entity with property `maxsize=0` – such as the `DummyCache` – will raise a `TypeError`.

### Str-only

The above concrete subclasses of `LmdbDict` support arbitrary serializable values in order to best mimic the functionality of the Python `dict`.

For use-cases supporting str-only (and/or bytes-only) values, all of the above concerns over serialization, caching and locking may be sidestepped.

`StrLmdbDict` provides the same full-featured `dict` interface to LMDB, but only for values of type `str` and `bytes`.

```python
from lmdb_dict import StrLmdbDict

StrLmdbDict('/path/to/db/directory/')
```

`StrLmdbDict` further differs from other subclasses of `LmdbDict` in that it accepts no `cache` argument, and may not perform caching.

## License

`lmdb-dict` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
