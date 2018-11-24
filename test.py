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


async def complete_one_at_at_time(task_states):
    history = []

    for task_state in task_states:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        history.append([state.started.done() for state in task_states])
        task_state.done.set_result(None)

    return history


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
    async def test_write_blocks_read(self):

        lock = TreeLock()

        # Same path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [path('/a/b/c')], 'write': []},
        ))

        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], False)
        self.assertEqual(started_history[1][1], True)

        # Descendant path
        task_states = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [path('/a/b/c/d/e')], 'write': []},
        ))
        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], False)
        self.assertEqual(started_history[1][1], True)

        # Ancestor path (ensures the order doesn't matter)
        task_states = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [path('/a')], 'write': []},
        ))
        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], False)
        self.assertEqual(started_history[1][1], True)

    @async_test
    async def test_write_blocks_write(self):

        lock = TreeLock()

        # Same path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [], 'write': [path('/a/b/c')]},
        ))

        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], False)
        self.assertEqual(started_history[1][1], True)

        # Descendant path
        task_states = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [], 'write': [path('/a/b/c/d/e')]},
        ))
        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], False)
        self.assertEqual(started_history[1][1], True)

        # Ancestor path (ensures the order doesn't matter)
        task_states = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [], 'write': [path('/a')]},
        ))
        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], False)
        self.assertEqual(started_history[1][1], True)

    @async_test
    async def test_write_allows_unrelated_write(self):

        lock = TreeLock()

        # Same path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [], 'write': [path('/a/b/e')]},
        ))

        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], True)

    @async_test
    async def test_write_allows_unrelated_read(self):

        lock = TreeLock()

        # Same path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [path('/a/b/e')], 'write': []},
        ))

        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], True)

    @async_test
    async def test_read_allows_read(self):

        lock = TreeLock()

        # Same path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c')], 'write': []},
            {'read': [path('/a/b/c')], 'write': []},
        ))

        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], True)

        # Descendant path
        task_states = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c')], 'write': []},
            {'read': [path('/a/b/c/d/e')], 'write': []},
        ))
        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], True)

        # Ancestor path (ensures the order doesn't matter)
        task_states = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c')], 'write': []},
            {'read': [path('/a')], 'write': []},
        ))
        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], True)

    @async_test
    async def test_read_allows_unrelated_read(self):

        lock = TreeLock()

        # Same path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c')], 'write': []},
            {'read': [path('/a/b/e')], 'write': []},
        ))

        self.assertEqual(started_history[0][0], True)
        self.assertEqual(started_history[0][1], True)

    # The below tests ensure that a block doesn't stop tasks queued after

    @async_test
    async def test_blocked_write_not_block_unrelated_read(self):

        lock = TreeLock()

        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [path('/a/b/c')], 'write': []},
            {'read': [path('/a/b/d')], 'write': []},
        ))
        self.assertEqual(started_history[0][2], True)

    @async_test
    async def test_blocked_write_not_block_unrelated_write(self):

        lock = TreeLock()

        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [], 'write': [path('/a/b/c')]},
            {'read': [], 'write': [path('/a/b/d')]},
        ))
        self.assertEqual(started_history[0][2], True)

    # The below tests are slightly strange edge-cases: where client codes
    # passes nodes in the same lineage

    @async_test
    async def test_writes_to_same_lineage(self):

        lock = TreeLock()

        # Same path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c'), path('/a/b/c')]},
        ))
        self.assertEqual(started_history[0][0], True)

        # Descendant path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c'), path('/a/b/c/d/e')]},
        ))
        self.assertEqual(started_history[0][0], True)

        # Ancestor path (ensures the order doesn't matter)
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [], 'write': [path('/a/b/c'), path('/a')]},
        ))
        self.assertEqual(started_history[0][0], True)

    @async_test
    async def test_reads_to_same_lineage(self):

        lock = TreeLock()

        # Same path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c'), path('/a/b/c')], 'write': []},
        ))
        self.assertEqual(started_history[0][0], True)

        # Descendant path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c'), path('/a/b/c/d/e')], 'write': []},
        ))
        self.assertEqual(started_history[0][0], True)

        # Ancestor path (ensures the order doesn't matter)
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c'), path('/a')], 'write': []},
        ))
        self.assertEqual(started_history[0][0], True)

    @async_test
    async def test_reads_and_write_to_same_lineage(self):

        lock = TreeLock()

        # Same path
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c')], 'write': [path('/a/b/c')]},
        ))
        self.assertEqual(started_history[0][0], True)

        # Write descendant path of read
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c')], 'write': [path('/a/b/c/d/e')]},
        ))
        self.assertEqual(started_history[0][0], True)

        # Write ancestor path of read
        started_history = await complete_one_at_at_time(create_tree_tasks(
            lock,
            {'read': [path('/a/b/c')], 'write': [path('/a')]},
        ))
        self.assertEqual(started_history[0][0], True)
