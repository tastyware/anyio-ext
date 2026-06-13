from typing import Any

import pytest
from anyio import sleep

from anyio_ext import cached, gather

pytestmark = pytest.mark.anyio


async def test_cache_hits_concurrent():
    counter = 0

    @cached(ttl=1)
    async def do_work() -> int:
        nonlocal counter
        await sleep(0)
        counter += 1
        return 22

    results = await gather(*[do_work() for _ in range(1_000)])
    assert sum(results) == 22_000
    assert counter == 1


async def test_cache_hits_sequential():
    calls = 0

    @cached
    async def work() -> int:
        nonlocal calls
        calls += 1
        return 42

    assert await work() == 42
    assert await work() == 42
    assert calls == 1


async def test_cache_none_value():
    calls = 0

    @cached(ttl=60)
    async def work() -> None:
        nonlocal calls
        calls += 1
        return None

    assert await work() is None
    assert await work() is None
    assert calls == 1


async def test_cache_different_args_different_entries():
    calls = 0

    @cached(ttl=60)
    async def work(x: int) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    assert await work(1) == 2
    assert await work(2) == 4
    assert await work(3) == 6
    assert calls == 3
    assert await work(1) == 2
    assert await work(2) == 4
    assert calls == 3


async def test_cache_canonical_args():
    calls = 0

    @cached(ttl=60)
    async def work(x: int, y: int = 10) -> int:
        nonlocal calls
        calls += 1
        return x + y

    assert await work(1) == 11
    assert await work(1, 10) == 11
    assert await work(1, y=10) == 11
    assert await work(x=1, y=10) == 11
    assert calls == 1


async def test_cache_complex_args():
    calls = 0

    @cached(ttl=60, key_fns={"items": lambda i: tuple(i)})
    async def work(items: list[int], options: tuple[str, int]) -> int:
        nonlocal calls
        calls += 1
        return sum(items) + options[1]

    assert await work([1, 2, 3], ("a", 10)) == 16
    assert await work([1, 2, 3], ("a", 10)) == 16
    assert calls == 1


async def test_cache_stampede():
    calls = 0

    @cached(ttl=60)
    async def work() -> int:
        nonlocal calls
        await sleep(0.01)  # give followers time to queue up behind us
        calls += 1
        return calls

    results = await gather(*[work() for _ in range(1000)])
    assert all(r == 1 for r in results)
    assert calls == 1


async def test_cache_ttl_expiry():
    calls = 0

    @cached(ttl=1)
    async def work() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await work() == 1
    assert await work() == 1  # cached
    await sleep(1)
    assert await work() == 2  # expired
    assert calls == 2


async def test_cache_exclude_arg():
    calls = 0

    @cached(ttl=60, exclude={"session"})
    async def work(x: int, session: str) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    assert await work(5, session="abc") == 10
    assert await work(5, session="xyz") == 10  # session ignored in key
    assert calls == 1
    assert await work(6, session="abc") == 12  # x changed → miss
    assert calls == 2


async def test_cache_key_fns():
    calls = 0

    @cached(ttl=60, key_fns={"user": lambda u: u["id"]})
    async def work(user: dict[str, Any]) -> int:
        nonlocal calls
        calls += 1
        return user["id"] * 2

    assert await work({"id": 1, "name": "alice"}) == 2
    assert await work({"id": 1, "name": "bob"}) == 2  # same id, hit
    assert calls == 1
    assert await work({"id": 2, "name": "carol"}) == 4
    assert calls == 2


async def test_cache_invalidate():
    calls = 0

    @cached(ttl=60)
    async def work(x: int) -> int:
        nonlocal calls
        calls += 1
        return x * 2

    assert await work(1) == 2
    assert await work(1) == 2
    assert calls == 1
    work.invalidate(1)
    assert await work(1) == 2
    assert calls == 2


async def test_cache_stats():
    calls = 0

    @cached
    async def work() -> int:
        nonlocal calls
        calls += 1
        return 42

    assert await work() == 42
    assert await work() == 42
    assert calls == 1
    assert work.hits == 1
    assert work.misses == 1
    assert len(work) == 1


async def test_cache_method():
    class MyClass:
        def __init__(self) -> None:
            self.calls = 0

        @cached
        async def work(self) -> int:
            self.calls += 1
            return 42

    mc = MyClass()
    assert await mc.work() == 42
    assert await mc.work() == 42
    assert mc.calls == 1
    assert MyClass.work.hits == 1
    assert MyClass.work.misses == 1
    assert len(MyClass.work) == 1
