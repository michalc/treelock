import asyncio
import contextlib
import weakref

from fifolock import FifoLock


class ReadAncestor(asyncio.Future):

    @staticmethod
    def is_compatible(holds):
        return not holds[Write]


class Read(asyncio.Future):

    @staticmethod
    def is_compatible(holds):
        return not holds[WriteAncestor] and not holds[Write]


class WriteAncestor(asyncio.Future):

    @staticmethod
    def is_compatible(holds):
        return not holds[Read] and not holds[Write]


class Write(asyncio.Future):

    @staticmethod
    def is_compatible(holds):
        return (
            not holds[ReadAncestor] and not holds[Read] and
            not holds[WriteAncestor] and not holds[Write]
        )


class TreeLock():

    def __init__(self):
        self._locks = weakref.WeakValueDictionary()

    def _with_locks(self, nodes, mode):
        return [
            (node, self._locks.setdefault(node, default=FifoLock()), mode)
            for node in nodes
        ]

    @contextlib.asynccontextmanager
    async def __call__(self, read, write):
        write_nodes = set(write)
        write_locks = self._with_locks(write_nodes, Write)

        write_ancestor_nodes = set(_flatten(node.parents for node in write_nodes)) \
            - write_nodes
        write_ancestor_locks = self._with_locks(write_ancestor_nodes, WriteAncestor)

        read_nodes = set(read) \
            - write_nodes - write_ancestor_nodes
        read_locks = self._with_locks(read_nodes, Read)

        read_ancestor_nodes = set(_flatten(node.parents for node in read_nodes)) \
            - write_nodes - write_ancestor_nodes - read_nodes
        read_ancestor_locks = self._with_locks(read_ancestor_nodes, ReadAncestor)

        sorted_locks = sorted(
            read_locks + read_ancestor_locks + write_locks + write_ancestor_locks)
        async with contextlib.AsyncExitStack() as stack:
            for _, lock, mode in sorted_locks:
                await stack.enter_async_context(lock(mode))

            yield


def _flatten(to_flatten):
    return [
        item for sub_list in to_flatten for item in sub_list
    ]
