from fsspec.spec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.dirfs import DirFileSystem
"""
Copy a file
"""
fs = LocalFileSystem()
assert isinstance(fs, AbstractFileSystem)
fs.cp('DEMO.md', 'DEMO_COPY.md', recursive=True)

"""
Copy a directory
"""
fs = LocalFileSystem()
assert isinstance(fs, AbstractFileSystem)
fs.cp('data', 'data_copy', recursive=True)