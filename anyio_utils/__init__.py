from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

from anyio import CapacityLimiter
from anyio.to_thread import run_sync

from anyio_utils.cache import cached
from anyio_utils.gather import gather
from anyio_utils.queue import Queue, Stack

VERSION = "0.1.0"
__version__ = VERSION


def asyncify(
    fn: Callable[P, R], limiter: CapacityLimiter | None = None
) -> Callable[P, Awaitable[R]]:
    """
    Taken from asyncer v0.0.8

    Take a blocking function and create an async one that receives the same
    positional and keyword arguments, and that when called, calls the original
    function in a worker thread using `anyio.to_thread.run_sync()`.

    If the task waiting for its completion is cancelled, the thread will still
    run its course but its result will be ignored.

    Example usage::

        def do_work(arg1, arg2, kwarg1="", kwarg2="") -> str:
            return "stuff"

        result = await to_thread.asyncify(do_work)(
            "spam",
            "ham",
            kwarg1="a",
            kwarg2="b"
        )
        print(result)

    :param fn: a blocking regular callable (e.g. a function)
    :param limiter: a CapacityLimiter instance to limit the number of concurrent
        threads running the blocking function.

    :return:
        An async function that takes the same positional and keyword arguments as the
        original one, that when called runs the same original function in a thread
        worker and returns the result.
    """

    @functools.wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        call = functools.partial(fn, *args, **kwargs)
        return await run_sync(call, abandon_on_cancel=True, limiter=limiter)

    return wrapper


__all__ = ["Queue", "Stack", "asyncify", "cached", "gather"]
