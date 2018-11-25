# treelock [![CircleCI](https://circleci.com/gh/michalc/treelock.svg?style=svg)](https://circleci.com/gh/michalc/treelock) [![Maintainability](https://api.codeclimate.com/v1/badges/d0174cccc3f8974fa4e7/maintainability)](https://codeclimate.com/github/michalc/treelock/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/d0174cccc3f8974fa4e7/test_coverage)](https://codeclimate.com/github/michalc/treelock/test_coverage)

Constant-time read/write sub-tree locking for asyncio Python. Suitable for large trees, when it's not feasible or desired to have the entire tree in memory at once.

Inspired by the work of [Ritik Malhotra](https://people.eecs.berkeley.edu/~kubitron/courses/cs262a-F14/projects/reports/project6_report.pdf).


## Usage

Each instance of `TreeLock` is callable, and returns an asynchronous context manager. In order to acquire a read (shared) lock on the sub-trees with root nodes in the iterable `read_roots`; and to acquire a write (exclusive) lock of the sub-trees with root nodes in the iterable `write_roots`, you must pass them to the instance of `TreeLock`:

```python
from treelock import TreeLock

lock = TreeLock()

def access(read_roots, write_roots):
  async with lock(read=read_roots, write=write_roots):
    # access the sub-trees
```

The lock is _not_ re-entrant: the same task attempting to enter multiple context managers with incompatible sub-trees will deadlock. Hence the locks for all the required sub-trees must be requested up-front.

A typical use-case will be for read/write (shared/exclusive) locking of a path in a filesystem hierarchy. For example, if treating S3 as a filesystem, but allowing what-whould-be non-atomic operations on folders.

For example, you could define `delete`, `write`, `rename`, `copy` and `read` operations on folders at certain <em>paths</em>, e.g. instances of `PurePosixPath`. A read lock of such a path should allow reads of the corresponding folder, but block all operations that would change it. A write lock should prevent all other access to that folder. You can do this using `TreeLock`, noting that each path is in fact a node in the tree of all possible paths.

```python
from treelock import TreeLock

lock = TreeLock()

async def delete(path):
  async with lock(read=[], write=[path]):
    ...

async def write(path, ...):
  async with lock(read=[], write=[path]):
    ...

async def rename(path_from, path_to):
  async with lock(read=[], write=[path_from, path_to]):
    ...

async def copy(path_from, path_to):
  async with lock(read=[path_from], write=[path_to]):
    ...

async def read(path):
  async with lock(read=[path], write=[]):
    ...
```

There is more information on this usage, as well as details of the underlying algorithm, at https://charemza.name/blog/posts/python/asyncio/s3-path-locking/.


## Required properties of the nodes

These are a subset of the properties of [PurePosixPath](https://docs.python.org/3/library/pathlib.html#pathlib.PurePosixPath).

- Each defines the `__cmp__` and  `__hash__` methods. These are used for dictionary and sets internally, so `__hash__` must be reasonable enough to to acheive constant-time behaviour.

- Each must define the `__lt__` method. This must be well-behaved, i.e. defines a total order between all possible nodes, otherwise deadlock can occur.

- Each has a property `parents` that is an iterator to the <em>ancestors</em> of the node, in any order. This is a slightly mis-named property, but this is consistent with PurePosixPath.

Note that a node does not need to be aware of its child nodes. This makes `TreeLock` suitable for locking sub-trees below a node without knowledge of the descendants of that node.


## Constant-time locking and unlocking

The number of operations to lock or unlock a node only depends on the ancestors of a node. Specifically, it does not increase as the number of descendants increase, nor does it increase with the number of locks currently being held.


## Running tests

```bash
python setup.py test
```
