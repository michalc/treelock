import asyncio
import collections
import pathlib
import unittest

from treelock import TreeLock


TaskState = collections.namedtuple('TaskState', ['started', 'done', 'task'])


def create_tree_tasks(lock, *nodes_list):
    async def access(nodes, started, done):
        async with lock(**nodes):
            started.set_result(None)
            await done

    def task(nodes):
        started = asyncio.Future()
        done = asyncio.Future()
        task = asyncio.create_task(access(nodes, started, done))
        return TaskState(started=started, done=done, task=task)

    return [task(nodes) for nodes in nodes_list]


def has_started(task_states):
    return [state.started.done() for state in task_states]


def async_test(func):
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


def path(path_str):
    return pathlib.PurePosixPath(path_str)


class TestTreeLock(unittest.TestCase):

    @async_test
    async def test_write_blocks_write(self):

        lock = TreeLock()

        # Same path
        task_states = create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [], 'write': [path('/a/b/c')]},
        )

        await asyncio.sleep(0)
        self.assertEqual(has_started(task_states)[0], True)
        self.assertEqual(has_started(task_states)[1], False)

        task_states[0].done.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(has_started(task_states)[1], True)

        task_states[1].done.set_result(None)

        # Descendant path
        task_states = create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [], 'write': [path('/a/b/c/d/e')]},
        )

        await asyncio.sleep(0)
        self.assertEqual(has_started(task_states)[0], True)
        self.assertEqual(has_started(task_states)[1], False)

        task_states[0].done.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(has_started(task_states)[1], True)

        task_states[1].done.set_result(None)

        # Ancestor path (ensures the order doesn't matter)
        task_states = create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [], 'write': [path('/a')]},
        )

        await asyncio.sleep(0)
        self.assertEqual(has_started(task_states)[0], True)
        self.assertEqual(has_started(task_states)[1], False)

        task_states[0].done.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(has_started(task_states)[1], True)

        task_states[1].done.set_result(None)
