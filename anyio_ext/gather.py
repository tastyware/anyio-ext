from collections.abc import Awaitable
from typing import Any, overload

from anyio import create_task_group

from anyio_ext.types import R, S, T, U, V, W


@overload
async def gather(
    awaitable1: Awaitable[R],
    awaitable2: Awaitable[S],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[R, S]: ...


@overload
async def gather(
    awaitable1: Awaitable[R],
    awaitable2: Awaitable[S],
    awaitable3: Awaitable[T],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[R, S, T]: ...


@overload
async def gather(
    awaitable1: Awaitable[R],
    awaitable2: Awaitable[S],
    awaitable3: Awaitable[T],
    awaitable4: Awaitable[U],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[R, S, T, U]: ...


@overload
async def gather(
    awaitable1: Awaitable[R],
    awaitable2: Awaitable[S],
    awaitable3: Awaitable[T],
    awaitable4: Awaitable[U],
    awaitable5: Awaitable[V],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[R, S, T, U, V]: ...


@overload
async def gather(
    awaitable1: Awaitable[R],
    awaitable2: Awaitable[S],
    awaitable3: Awaitable[T],
    awaitable4: Awaitable[U],
    awaitable5: Awaitable[V],
    awaitable6: Awaitable[W],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[R, S, T, U, V, W]: ...


@overload
async def gather(
    *awaitables: Awaitable[R], return_exceptions: bool = False
) -> tuple[R, ...]: ...


async def gather(
    *awaitables: Awaitable[Any], return_exceptions: bool = False
) -> tuple[Any, ...]:
    """
    Re-implementation of :func:`asyncio.gather` that uses an anyio task group.

    :param return_exceptions: whether to return exceptions instead of raising them
    """
    if not awaitables:
        return ()
    results: list[Any] = [None] * len(awaitables)

    async def runner(awaitable: Awaitable[Any], i: int) -> None:
        try:
            results[i] = await awaitable
        except Exception as exc:
            if not return_exceptions:
                raise
            results[i] = exc

    async with create_task_group() as tg:
        for i, awaitable in enumerate(awaitables):
            tg.start_soon(runner, awaitable, i)

    return tuple(results)
