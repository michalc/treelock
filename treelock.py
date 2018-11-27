import asyncio
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

    def __call__(self, read, write):
        return TreeLockContextManager(self._locks, read, write)


class TreeLockContextManager():

    def __init__(self, locks, read, write):
        self._locks = locks

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

        self._sorted_locks = sorted(
            read_locks + read_ancestor_locks + write_locks + write_ancestor_locks)

    def _with_locks(self, nodes, mode):
        return [
            (node, self._locks.setdefault(node, default=FifoLock()), mode)
            for node in nodes
        ]

    async def __aenter__(self):
        self._acquired = []
        try:
            for _, lock, mode in self._sorted_locks:
                lock_mode = lock(mode)
                await lock_mode.__aenter__()
                self._acquired.append(lock_mode)

        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def __aexit__(self, _, __, ___):
        for lock_mode in reversed(self._acquired):
            await lock_mode.__aexit__(None, None, None)


def _flatten(to_flatten):
    return [
        item for sub_list in to_flatten for item in sub_list
    ]
