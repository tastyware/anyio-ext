from __future__ import annotations

import inspect
import math
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from datetime import timedelta
from functools import update_wrapper
from typing import Any, Concatenate, Generic, overload
from weakref import WeakValueDictionary

from anyio import Lock, current_time

from anyio_ext.types import P, Q, R, S


class _BoundMethod(Generic[P, R]):
    def __init__(self, cached: CachedFunction[..., R], instance: object) -> None:
        self._cached = cached
        self._instance = instance

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return await self._cached(self._instance, *args, **kwargs)

    def invalidate(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """
        Invalidate the key built from the given arguments.
        """
        self._cached.invalidate(self._instance, *args, **kwargs)


class CachedFunction(Generic[P, R]):
    def __init__(
        self,
        fn: Callable[P, Awaitable[R]],
        *,
        max_keys: int | None,
        ttl: timedelta | int | None,
        exclude: set[str] | None,
        key_fns: dict[str, Callable[[Any], Any]] | None,
    ) -> None:
        #: number of cache hits
        self.hits = 0
        #: number of cache misses
        self.misses = 0
        self._fn = fn
        self._sig = inspect.signature(fn)
        self._ttl = (
            round(ttl.total_seconds())
            if isinstance(ttl, timedelta)
            else ttl or math.inf
        )
        self._cache = OrderedDict[int, tuple[float, Any]]()
        self._exclude = exclude or set()
        self._key_fns = key_fns or {}
        self._max_keys = max_keys
        update_wrapper(self, fn)
        self.__signature__ = self._sig
        self._locks = WeakValueDictionary[int, Lock]()

    def build_key(self, *args: P.args, **kwargs: P.kwargs) -> int:
        bound = self._sig.bind(*args, **kwargs)
        bound.apply_defaults()
        canonical = tuple(
            self._key_fns[k](v) if k in self._key_fns else v
            for k, v in bound.arguments.items()
            if k not in self._exclude
        )
        return hash(canonical)

    def invalidate(self, *args: P.args, **kwargs: P.kwargs) -> None:
        """
        Invalidate the key built from the given arguments.
        """
        key = self.build_key(*args, **kwargs)
        self._cache.pop(key, None)

    def _store(self, key: int, res: Any) -> None:
        self._cache[key] = res
        self._cache.move_to_end(key)
        if self._max_keys is not None and len(self._cache) > self._max_keys:
            self._cache.popitem(last=False)

    def __len__(self) -> int:
        return len(self._cache)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        key = self.build_key(*args, **kwargs)
        res = self._cache.get(key)
        if res and res[0] > current_time():
            self._cache.move_to_end(key)
            self.hits += 1
            return res[1]
        lock = self._locks.get(key)
        if lock is None:
            lock = self._locks[key] = Lock()
        async with lock:
            res = self._cache.get(key)
            if res and res[0] > current_time():
                self._cache.move_to_end(key)
                self.hits += 1
                return res[1]
            self.misses += 1
            val = await self._fn(*args, **kwargs)
            self._store(key, (current_time() + self._ttl, val))
            return val

    @overload
    def __get__(
        self, instance: None, owner: type | None = None
    ) -> CachedFunction[P, R]: ...

    @overload
    def __get__(
        self: CachedFunction[Concatenate[S, Q], R],
        instance: S,
        owner: type | None = None,
    ) -> _BoundMethod[Q, R]: ...

    def __get__(self, instance: Any, owner: type | None = None) -> Any:
        if instance is None:  # accessed on the class, not an instance
            return self
        return _BoundMethod(self, instance)


@overload
def cached(fn: Callable[P, Awaitable[R]], /) -> CachedFunction[P, R]: ...


@overload
def cached(
    *,
    max_keys: int | None = None,
    ttl: timedelta | int | None = None,
    exclude: set[str] | None = None,
    key_fns: dict[str, Callable[[Any], Any]] | None = ...,
) -> Callable[[Callable[P, Awaitable[R]]], CachedFunction[P, R]]: ...


def cached(
    fn: Callable[P, Awaitable[R]] | None = None,
    *,
    max_keys: int | None = None,
    ttl: timedelta | int | None = None,
    exclude: set[str] | None = None,
    key_fns: dict[str, Callable[[Any], Any]] | None = None,
) -> Any:
    """
    Cache keys are built by hashing the arguments. Use ``exclude`` to drop arguments
    from the key, or ``key_fns`` to transform them first (handy for arguments that
    aren't hashable or should be excluded, eg database sessions, HTTP clients,
    SQLAlchemy objects).

    On instance methods, `self` is part of the key by default, so caching is per-
    instance. Unlike :func:`functools.lru_cache`, this does not leak instances: keys
    store a *hash* of arguments, not references. If you wish to share the cache across
    instances, exclude the `self` parameter from key generation.

    :param max_keys: maximum number of keys to store in the cache
    :param ttl: duration to cache results, defaults to forever
    :param exclude: argument names to exclude from cache key generation
    :param key_fns: mapping of argument name -> lambda to modify argument
    """

    def decorator(_fn: Callable[P, Awaitable[R]]) -> CachedFunction[P, R]:
        return CachedFunction(
            _fn,
            max_keys=max_keys,
            ttl=ttl,
            exclude=exclude,
            key_fns=key_fns,
        )

    if fn is None:
        return decorator
    return decorator(fn)
