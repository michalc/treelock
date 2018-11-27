import asyncio
import collections
from pathlib import PurePosixPath as path
import unittest

from treelock import TreeLock


TaskState = collections.namedtuple('TaskState', ['acquired', 'done', 'task'])


def create_tree_tasks(*lock_modes):
    async def access(lock_mode, acquired, done):
        async with lock_mode:
            acquired.set_result(None)
            await done

    def task(lock_mode):
        acquired = asyncio.Future()
        done = asyncio.Future()
        task = asyncio.ensure_future(access(lock_mode, acquired, done))
        return TaskState(acquired=acquired, done=done, task=task)

    return [task(lock_mode) for lock_mode in lock_modes]


async def mutate_tasks_in_sequence(task_states, *funcs):
    history = []

    for func in funcs + (null, ):
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        history.append([state.acquired.done() for state in task_states])
        func(task_states)

    return history


def cancel(i):
    def func(tasks):
        tasks[i].task.cancel()
    return func


def complete(i):
    def func(tasks):
        tasks[i].done.set_result(None)
    return func


def exception(i, exception):
    def func(tasks):
        tasks[i].done.set_exception(exception)
    return func


def null(_):
    pass


def async_test(func):
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


class TestTreeLock(unittest.TestCase):

    @async_test
    async def test_write_blocks_read(self):

        lock = TreeLock()

        # Same path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[path('/a/b/c')], write=[]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, False])
        self.assertEqual(acquired_history[1], [True, True])

        # Descendant path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[path('/a/b/c/d/e')], write=[]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, False])
        self.assertEqual(acquired_history[1], [True, True])

        # Ancestor path (ensures the order doesn't matter)
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[path('/a')], write=[]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, False])
        self.assertEqual(acquired_history[1], [True, True])

    @async_test
    async def test_write_blocks_write(self):

        lock = TreeLock()

        # Same path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a/b/c')]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, False])
        self.assertEqual(acquired_history[1], [True, True])

        # Descendant path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a/b/c/d/e')]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, False])
        self.assertEqual(acquired_history[1], [True, True])

        # Ancestor path (ensures the order doesn't matter)
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a')]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, False])
        self.assertEqual(acquired_history[1], [True, True])

    @async_test
    async def test_write_allows_unrelated_write(self):

        lock = TreeLock()

        # Same path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a/b/e')]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, True])

    @async_test
    async def test_write_allows_unrelated_read(self):

        lock = TreeLock()

        # Same path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[path('/a/b/e')], write=[]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, True])

    @async_test
    async def test_read_allows_read(self):

        lock = TreeLock()

        # Same path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c')], write=[]),
            lock(read=[path('/a/b/c')], write=[]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, True])

        # Descendant path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c')], write=[]),
            lock(read=[path('/a/b/c/d/e')], write=[]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, True])

        # Ancestor path (ensures the order doesn't matter)
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c')], write=[]),
            lock(read=[path('/a')], write=[]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, True])

    @async_test
    async def test_read_allows_unrelated_read(self):

        lock = TreeLock()

        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c')], write=[]),
            lock(read=[path('/a/b/e')], write=[]),
        ), complete(0), complete(1))
        self.assertEqual(acquired_history[0], [True, True])

    # The below tests ensure that a block doesn't stop tasks queued after

    @async_test
    async def test_blocked_read_not_block_unrelated_read(self):

        lock = TreeLock()

        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[path('/a/b/c')], write=[]),
            lock(read=[path('/a/b/d')], write=[]),
        ), complete(0), complete(1), complete(2))
        self.assertEqual(acquired_history[0], [True, False, True])
        self.assertEqual(acquired_history[1], [True, True, True])

    @async_test
    async def test_blocked_write_not_block_unrelated_write(self):

        lock = TreeLock()

        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a/b/d')]),
        ), complete(0), complete(1), complete(2))
        self.assertEqual(acquired_history[0], [True, False, True])
        self.assertEqual(acquired_history[1], [True, True, True])

    # Ensure cancellation after acquisition unblocks

    @async_test
    async def test_cancellation_after_acquisition_unblocks_write(self):

        lock = TreeLock()

        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a/b/c/d')]),
        ), cancel(0), complete(1))
        self.assertEqual(acquired_history[0], [True, False])
        self.assertEqual(acquired_history[1], [True, True])

    @async_test
    async def test_cancellation_after_acquisition_unblocks_read(self):

        lock = TreeLock()

        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[path('/a/b/c/d')], write=[]),
        ), cancel(0), complete(1))
        self.assertEqual(acquired_history[0], [True, False])
        self.assertEqual(acquired_history[1], [True, True])

    # Ensure cancellation before acquisition unblocks

    @async_test
    async def test_cancellation_before_acquisition_unblocks_write(self):

        lock = TreeLock()

        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a/b/c/d')]),
            lock(read=[], write=[path('/a/b/c/d')]),
        ), cancel(1), complete(0), null, complete(2))
        self.assertEqual(acquired_history[0], [True, False, False])
        self.assertEqual(acquired_history[2], [True, False, True])

    @async_test
    async def test_cancellation_before_acquisition_unblocks_read(self):

        lock = TreeLock()

        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a/b/c/d')]),
            lock(read=[path('/a/b/c/d')], write=[]),
        ), cancel(1), complete(0), null, complete(2))
        self.assertEqual(acquired_history[0], [True, False, False])
        self.assertEqual(acquired_history[2], [True, False, True])

    # Ensure exception after acquisition unblocks

    @async_test
    async def test_exception_after_acquisition_unblocks_write(self):

        lock = TreeLock()

        tasks = create_tree_tasks(
            lock(read=[], write=[path('/a/b/c')]),
            lock(read=[], write=[path('/a/b/c/d')]),
        )
        exp = Exception('Raised exception')
        acquired_history = await mutate_tasks_in_sequence(
            tasks,
            exception(0, exp), complete(1))

        self.assertEqual(tasks[0].task.exception(), exp)
        self.assertEqual(acquired_history[0], [True, False])
        self.assertEqual(acquired_history[1], [True, True])

    # The below tests are slightly strange edge-cases: where client codes
    # passes nodes in the same lineage

    @async_test
    async def test_writes_to_same_lineage(self):

        lock = TreeLock()

        # Same path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c'), path('/a/b/c')]),
        ), complete(0))
        self.assertEqual(acquired_history[0], [True])

        # Descendant path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c'), path('/a/b/c/d/e')]),
        ), complete(0))
        self.assertEqual(acquired_history[0], [True])

        # Ancestor path (ensures the order doesn't matter)
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[], write=[path('/a/b/c'), path('/a')]),
        ), complete(0))
        self.assertEqual(acquired_history[0], [True])

    @async_test
    async def test_reads_to_same_lineage(self):

        lock = TreeLock()

        # Same path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c'), path('/a/b/c')], write=[]),
        ), complete(0))
        self.assertEqual(acquired_history[0], [True])

        # Descendant path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c'), path('/a/b/c/d/e')], write=[]),
        ), complete(0))
        self.assertEqual(acquired_history[0], [True])

        # Ancestor path (ensures the order doesn't matter)
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c'), path('/a')], write=[]),
        ), complete(0))
        self.assertEqual(acquired_history[0], [True])

    @async_test
    async def test_reads_and_write_to_same_lineage(self):

        lock = TreeLock()

        # Same path
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c')], write=[path('/a/b/c')]),
        ), complete(0))
        self.assertEqual(acquired_history[0], [True])

        # Write descendant path of read
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c')], write=[path('/a/b/c/d/e')]),
        ), complete(0))
        self.assertEqual(acquired_history[0], [True])

        # Write ancestor path of read
        acquired_history = await mutate_tasks_in_sequence(create_tree_tasks(
            lock(read=[path('/a/b/c')], write=[path('/a')]),
        ), complete(0))
        self.assertEqual(acquired_history[0], [True])
