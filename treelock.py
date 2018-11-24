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


class PathLock():

    def __init__(self):
        self._locks = weakref.WeakValueDictionary()

    @staticmethod
    def _sort_key(path_lock):
        return len(path_lock[0].parents), path_lock[0].as_posix()

    @staticmethod
    def _flatten(to_flatten):
        return [
            item for sub_list in to_flatten for item in sub_list
        ]

    def _with_locks(self, paths, mode):
        return [
            (path, self._locks.setdefault(path, default=FifoLock()), mode)
            for path in paths
        ]

    @contextlib.asynccontextmanager
    async def __call__(self, read, write):
        write_paths = set(write)
        write_locks = self._with_locks(write_paths, Write)

        write_ancestor_paths = set(self._flatten(path.parents for path in write_paths)) \
            - write_paths
        write_ancestor_locks = self._with_locks(write_ancestor_paths, WriteAncestor)

        read_paths = set(read) \
            - write_paths - write_ancestor_paths
        read_locks = self._with_locks(read_paths, Read)

        read_ancestor_paths = set(self._flatten(path.parents for path in read_paths)) \
            - write_paths - write_ancestor_paths - read_paths
        read_ancestor_locks = self._with_locks(read_ancestor_paths, ReadAncestor)

        sorted_locks = sorted(
            read_locks + read_ancestor_locks + write_locks + write_ancestor_locks,
            key=self._sort_key)
        async with contextlib.AsyncExitStack() as stack:
            for _, lock, mode in sorted_locks:
                await stack.enter_async_context(lock(mode))

            yield
