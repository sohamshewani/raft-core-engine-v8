import asyncio
import struct
import logging
from typing import Dict, Any, Optional, Callable, Awaitable, List

logger = logging.getLogger("raft.concurrency")

class RaftLockManager:
    """
    Fine-grained synchronization primitives protecting volatile shared state 
    across the Raft node lifecycle, strictly adhering to core invariants:
    - current_term
    - voted_for
    - commit_index
    - last_applied
    """
    def __init__(self) -> None:
        self._term_lock: asyncio.Lock = asyncio.Lock()
        self._commit_lock: asyncio.Lock = asyncio.Lock()
        self._state_lock: asyncio.Lock = asyncio.Lock()
        
        self._commit_condition: asyncio.Condition = asyncio.Condition(self._commit_lock)
        self._apply_condition: asyncio.Condition = asyncio.Condition(self._state_lock)

    async def acquire_term(self) -> asyncio.Lock:
        return self._term_lock

    async def acquire_commit(self) -> asyncio.Lock:
        return self._commit_lock

    async def acquire_state(self) -> asyncio.Lock:
        return self._state_lock

    @property
    def commit_condition(self) -> asyncio.Condition:
        return self._commit_condition

    @property
    def apply_condition(self) -> asyncio.Condition:
        return self._apply_condition


class AsyncExecutorPool:
    """
    Manages background thread-pool execution for blocking operations 
    such as disk I/O, mmap manipulation, and WriteAheadLog fsync batches.
    """
    def __init__(self, max_workers: int = 4) -> None:
        from concurrent.futures import ThreadPoolExecutor
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="raft-disk-worker"
        )
        self._loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    async def run_blocking(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        import functools
        partial_func = functools.partial(func, *args, **kwargs)
        return await self._loop.run_in_executor(self._executor, partial_func)

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)


class Channel:
    """
    An unbounded asynchronous FIFO channel for inter-task communication 
    and RPC multiplexing within the concurrency layer.
    """
    def __init__(self) -> None:
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._closed: bool = False

    async def send(self, item: Any) -> None:
        if self._closed:
            raise RuntimeError("Cannot send item to a closed Channel.")
        await self._queue.put(item)

    async def recv(self) -> Any:
        if self._closed and self._queue.empty():
            raise EOFError("Channel is closed and empty.")
        return await self._queue.get()

    def close(self) -> None:
        self._closed = True

    @property
    def empty(self) -> bool:
        return self._queue.empty()


class ConcurrencySupervisor:
    """
    Supervises background tasks, lifecycle handlers, and graceful cancellations 
    across the raft-core-engine concurrency boundary.
    """
    def __init__(self) -> None:
        self._tasks: List[asyncio.Task] = []
        self._shutdown_event: asyncio.Event = asyncio.Event()

    def spawn(self, coro: Awaitable[Any], name: Optional[str] = None) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._tasks.append(task)
        task.add_done_callback(self._task_done_callback)
        return task

    def _task_done_callback(self, task: asyncio.Task) -> None:
        if task in self._tasks:
            self._tasks.remove(task)
        try:
            exc = task.exception()
            if exc is not None:
                logger.error(f"Task {task.get_name()} failed with exception: {exc}", exc_info=exc)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error handling task callback: {e}", exc_info=True)

    async def shutdown_all(self, timeout: float = 5.0) -> None:
        self._shutdown_event.set()
        if not self._tasks:
            return
        
        for task in self._tasks:
            if not task.done():
                task.cancel()

        _, pending = await asyncio.wait(self._tasks, timeout=timeout)
        for task in pending:
            logger.warning(f"Task {task.get_name()} did not terminate gracefully; forcing abort.")
            task.cancel()
        self._tasks.clear()