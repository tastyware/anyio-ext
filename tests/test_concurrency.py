import pytest
from anyio import sleep

from anyio_ext.concurrency import as_completed, gather

pytestmark = pytest.mark.anyio


# --- gather ---
async def test_gather_preserves_order():
    async def val(x: int) -> int:
        # later-passed coros finish first; result order must still match input
        await sleep((5 - x) * 0.01)
        return x

    assert await gather(val(0), val(1), val(2), val(3), val(4)) == (0, 1, 2, 3, 4)


async def test_gather_mixed_types():
    async def num() -> int:
        return 1

    async def text() -> str:
        return "a"

    assert await gather(num(), text()) == (1, "a")


async def test_gather_empty():
    assert await gather() == ()


async def test_gather_propagates_exception():
    async def boom() -> int:
        raise ValueError("boom")

    async def ok() -> int:
        await sleep(0)
        return 1

    with pytest.raises(BaseExceptionGroup) as exc:
        await gather(ok(), boom())
    assert any(isinstance(e, ValueError) for e in exc.value.exceptions)


# --- as_completed ---
async def test_as_completed_yields_in_finish_order():
    async def val(x: int, delay: float) -> int:
        await sleep(delay)
        return x

    results: list[int] = []
    async with as_completed(val(0, 0.03), val(1, 0.01), val(2, 0.02)) as stream:
        async for r in stream:
            results.append(r)

    assert results == [1, 2, 0]


async def test_as_completed_returns_all_results():
    async def val(x: int) -> int:
        await sleep(0)
        return x

    got: set[int] = set()
    async with as_completed(*(val(i) for i in range(10))) as stream:
        async for r in stream:
            got.add(r)

    assert got == set(range(10))


async def test_as_completed_propagates_exception():
    async def boom() -> int:
        raise ValueError("boom")

    with pytest.raises(BaseExceptionGroup) as exc:
        async with as_completed(boom()) as stream:
            async for _ in stream:
                pass
    assert any(isinstance(e, ValueError) for e in exc.value.exceptions)
