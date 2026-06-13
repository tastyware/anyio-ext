import pytest
from anyio import WouldBlock, create_task_group, fail_after, move_on_after, sleep

from anyio_ext.queue import Queue, Stack

pytestmark = pytest.mark.anyio


# --- ordering ---
async def test_queue_is_fifo():
    q = Queue[int]()
    for i in range(5):
        await q.push(i)
    assert [await q.pop() for _ in range(5)] == [0, 1, 2, 3, 4]


async def test_stack_is_lifo():
    s = Stack[int]()
    for i in range(5):
        await s.push(i)
    assert [await s.pop() for _ in range(5)] == [4, 3, 2, 1, 0]


# --- nowait paths ---
async def test_pop_nowait_empty_raises():
    with pytest.raises(WouldBlock):
        Queue[int]().pop_nowait()


async def test_push_nowait_full_raises():
    q = Queue[int](max_size=1)
    q.push_nowait(1)
    with pytest.raises(WouldBlock):
        q.push_nowait(2)


# --- blocking + handoff ---
async def test_pop_blocks_then_receives():
    q = Queue[int]()
    got = []

    async def consumer():
        got.append(await q.pop())

    with fail_after(1):
        async with create_task_group() as tg:
            tg.start_soon(consumer)
            await sleep(0.01)  # let consumer park
            await q.push(7)
    assert got == [7]


async def test_direct_handoff_skips_buffer():
    q = Queue[int]()
    got = []

    async def consumer():
        got.append(await q.pop())

    with fail_after(1):
        async with create_task_group() as tg:
            tg.start_soon(consumer)
            await sleep(0.01)  # consumer parks in _getters
            q.push_nowait(42)  # hand straight to it
            assert len(q) == 0  # nothing buffered
    assert got == [42]


async def test_push_blocks_until_consumer_makes_room():
    q = Queue[int](max_size=1)
    await q.push(1)  # full
    order = []

    async def producer():
        await q.push(2)
        order.append("appended")

    with fail_after(1):
        async with create_task_group() as tg:
            tg.start_soon(producer)
            await sleep(0.01)
            assert order == []  # producer is blocked
            assert await q.pop() == 1  # frees a slot -> admits producer's 2
            await sleep(0.01)
            assert order == ["appended"]
    assert await q.pop() == 2


# --- cancellation safety (the bug that bit you) ---
async def test_cancelled_pop_deregisters_itself():
    q = Queue[int]()
    with move_on_after(0.05):
        await q.pop()
    assert len(q._getters) == 0  # waiter cleaned up, not left as a dead event


async def test_cancelled_waiter_does_not_swallow_item():
    q = Queue[int]()
    got = []

    async def doomed():
        with move_on_after(0.05):
            await q.pop()  # cancelled before any item arrives

    async def survivor():
        got.append(await q.pop())

    with fail_after(1):
        async with create_task_group() as tg:
            tg.start_soon(doomed)
            await sleep(0.01)  # doomed parks first (head of _getters)
            tg.start_soon(survivor)
            await sleep(0.01)  # survivor parks second
            await sleep(0.06)  # doomed times out and deregisters
            await q.push(99)  # must reach survivor, not the dead waiter
    assert got == [99]


# --- semantics of 0 / None ---
async def test_zero_size_is_rendezvous():
    q = Queue[int](max_size=0)
    with pytest.raises(WouldBlock):
        q.push_nowait(1)  # no consumer waiting -> can't buffer
    got = []

    async def consumer():
        got.append(await q.pop())

    with fail_after(1):
        async with create_task_group() as tg:
            tg.start_soon(consumer)
            await sleep(0.01)
            await q.push(5)  # rendezvous hand-off
    assert got == [5]
    assert len(q) == 0


async def test_unbounded_never_blocks_producer():
    q = Queue[int]()
    with fail_after(1):
        for i in range(1000):
            await q.push(i)
    assert len(q) == 1000


# --- inspectors ---
async def test_inspectors():
    q = Queue[int](max_size=2)
    assert q.empty() and not q.full() and len(q) == 0
    await q.push(1)
    assert 1 in q and not q.empty()
    await q.push(2)
    assert q.full()


# --- no loss / no dup under producer+consumer pressure ---
async def test_no_items_lost_under_pressure():
    q = Queue[int](max_size=10)
    n = 500
    got = []

    async def consumer():
        for _ in range(n):
            got.append(await q.pop())

    with fail_after(5):
        async with create_task_group() as tg:
            tg.start_soon(consumer)
            for i in range(n):
                await q.push(i)
    assert got == list(range(n))  # single consumer -> FIFO order preserved
