import asyncio
from collections import deque
from heapq import merge
from weakref import WeakValueDictionary

from fifolock import FifoLock


__all__ = ['TreeLock']


class ReadAncestor(asyncio.Future):

    @staticmethod
    def is_compatible(holds):
        return not holds[Write]


class Read(asyncio.Future):

    @staticmethod
    def is_compatible(holds):
        return not holds[WriteAncestor] and not holds[Write] and not holds[ReadAndWriteAncestor]


class WriteAncestor(asyncio.Future):

    @staticmethod
    def is_compatible(holds):
        return not holds[Read] and not holds[Write] and not holds[ReadAndWriteAncestor]


class Write(asyncio.Future):

    @staticmethod
    def is_compatible(holds):
        return (
            not holds[ReadAncestor] and not holds[Read] and
            not holds[WriteAncestor] and not holds[Write] and
            not holds[ReadAndWriteAncestor]
        )


class ReadAndWriteAncestor(asyncio.Future):

    @staticmethod
    def is_compatible(holds):
        return Read.is_compatible(holds) and WriteAncestor.is_compatible(holds)


class TreeLock():

    def __init__(self):
        self._locks = WeakValueDictionary()

    def __call__(self, read, write):
        return TreeLockContextManager(self._locks, read, write)


class TreeLockContextManager():

    def __init__(self, locks, read, write):
        self._locks = locks
        self._read = read
        self._write = write
        self._acquired = deque()

    WriteAncestorAndReadSet = {WriteAncestor, Read}
    Last = (None, None, None)

    async def __aenter__(self):
        def with_locks(nodes, mode):
            return (
                (node, self._locks.setdefault(node, default=FifoLock()), mode)
                for node in nodes
            )

        write_locks = [with_locks([node], Write) for node in self._write]
        write_ancestor_locks = [with_locks(node.parents, WriteAncestor) for node in self._write]

        read_locks = [with_locks([node], Read) for node in self._read]
        read_ancestor_locks = [with_locks(node.parents, ReadAncestor) for node in self._read]

        all_locks = write_locks + write_ancestor_locks + read_locks + read_ancestor_locks
        sorted_locks = list(merge(*all_locks, key=lambda lock: lock[0], reverse=True))
        sorted_locks.append(self.Last)

        lock_modes = set()
        previous = None
        for index, (node, lock, mode) in enumerate(sorted_locks):
            if index == 0:
                previous = lock

            if previous == lock:
                lock_modes.add(mode)
                continue

            if Write in lock_modes:
                final_mode = Write
            elif len(self.WriteAncestorAndReadSet.intersection(lock_modes)) == 2:
                final_mode = ReadAndWriteAncestor
            elif WriteAncestor in lock_modes:
                final_mode = WriteAncestor
            elif Read in lock_modes:
                final_mode = Read
            else:
                final_mode = ReadAncestor

            lock_modes = {mode}
            lock_mode = previous(final_mode)
            try:
                await lock_mode.__aenter__()
            except BaseException:
                await self.__aexit__(None, None, None)
                raise

            # We must keep a reference to the lock until we've unlocked to
            # avoid it being garbage collected from the weakref dict
            self._acquired.append((previous, lock_mode))

            previous = lock

    async def __aexit__(self, _, __, ___):
        while self._acquired:
            await self._acquired.pop()[1].__aexit__(None, None, None)
