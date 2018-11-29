import asyncio
import heapq
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

        write_locks = [self._with_locks([node], Write) for node in write]
        write_ancestor_locks = [self._with_locks(node.parents, WriteAncestor) for node in write]

        read_locks = [self._with_locks([node], Read) for node in read]
        read_ancestor_locks = [self._with_locks(node.parents, ReadAncestor) for node in read]

        all_locks = write_locks + write_ancestor_locks + read_locks + read_ancestor_locks 
        self._sorted_locks = deduplicate_sorted(
            heapq.merge(*all_locks, key=lambda lock: lock[0], reverse=True))

    def _with_locks(self, nodes, mode):
        return (
            (node, self._locks.setdefault(node, default=FifoLock()), mode)
            for node in nodes
        )

    async def __aenter__(self):
        self._acquired = []
        try:
            for _, lock, mode in self._sorted_locks:
                lock_mode = lock(mode)
                await lock_mode.__aenter__()
                # We must keep a reference to the lock until we've unlocked to
                # avoid it being garbage collected from the weakref dict
                self._acquired.append((lock, lock_mode))

        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def __aexit__(self, _, __, ___):
        for _, lock_mode in reversed(self._acquired):
            await lock_mode.__aexit__(None, None, None)
        self._acquired = []


def deduplicate_sorted(iterator):
    previous = None
    for index, value in enumerate(iterator):
        if index == 0 or value[0] != previous[0]:
            yield value
        previous = value
