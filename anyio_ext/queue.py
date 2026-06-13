from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Generic

from anyio import Event, TaskInfo, WouldBlock, get_current_task
from anyio.lowlevel import checkpoint

from anyio_ext.types import T


@dataclass(eq=False)
class _ItemReceiver(Generic[T]):
    task_info: TaskInfo = field(init=False, default_factory=get_current_task)
    item: T = field(init=False)


class Queue(Generic[T]):
    """
    A simple FIFO queue implementation.
    """

    def __init__(self, max_size: int | None = None):
        self._size = max_size
        self._queue: deque[T] = deque()
        self._getters = OrderedDict[Event, _ItemReceiver[T]]()
        self._putters = OrderedDict[Event, T]()

    def empty(self) -> bool:
        return not self._queue

    def full(self) -> bool:
        return self._size is not None and len(self._queue) >= self._size

    def __len__(self) -> int:
        return len(self._queue)

    def __contains__(self, item: T) -> bool:
        return item in self._queue

    def _add_item(self, item: T) -> None:
        self._queue.appendleft(item)

    def pop_nowait(self) -> T:
        """
        Get an item from the queue. Raises :exc:`anyio.WouldBlock` if empty.
        """
        if self._putters:
            ev, item = self._putters.popitem(last=False)
            self._add_item(item)
            ev.set()
        if self._queue:
            return self._queue.pop()
        raise WouldBlock()

    async def pop(self) -> T:
        """
        Get an item from the queue, blocking until successful.
        """
        await checkpoint()
        try:
            return self.pop_nowait()
        except WouldBlock:
            ev = Event()
            slot: _ItemReceiver[T] = _ItemReceiver()
            self._getters[ev] = slot
            try:
                await ev.wait()
            finally:
                self._getters.pop(ev, None)
            return slot.item

    def push_nowait(self, item: T) -> None:
        """
        Add a new item to the queue. Raises :exc:`anyio.WouldBlock` if already full.
        """
        # hand straight to a live waiting consumer, skipping doomed ones
        while self._getters:
            ev, slot = self._getters.popitem(last=False)
            if not slot.task_info.has_pending_cancellation():
                slot.item = item
                ev.set()
                return
        if not self.full():
            self._add_item(item)
        else:
            raise WouldBlock

    async def push(self, item: T) -> None:
        """
        Add a new item to the queue, blocking until successful.
        """
        await checkpoint()
        try:
            self.push_nowait(item)
        except WouldBlock:
            ev = Event()
            self._putters[ev] = item
            try:
                await ev.wait()
            except BaseException:
                self._putters.pop(ev, None)
                raise


class Stack(Queue[T]):
    """
    A simple LIFO queue implementation.
    """

    def _add_item(self, item: T) -> None:
        return self._queue.append(item)
