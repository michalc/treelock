import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='treelock',
    version='0.0.11',
    author='Michal Charemza',
    author_email='michal@charemza.name',
    description='Constant-time read/write sub-tree locking for asyncio Python',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/michalc/treelock',
    py_modules=[
        'treelock',
    ],
    python_requires='>=3.7',
    install_requires=[
        'fifolock',
    ],
    test_suite='test',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Framework :: AsyncIO',
    ],
)
