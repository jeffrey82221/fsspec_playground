from fsspec.spec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.dirfs import DirFileSystem
"""
Read and Write from/to Local FileSystem
"""
fs = LocalFileSystem()
assert isinstance(fs, AbstractFileSystem)
with fs.open('DEMO.md', 'w') as f:
    f.write('hello')

with fs.open('DEMO.md', 'r') as f:
    print(f.read())
print(fs.info('DEMO.md'))
"""
Change root_path of a filesystem
"""
fs = DirFileSystem('./data')
assert isinstance(fs, AbstractFileSystem)
with fs.open('DEMO.md', 'w') as f:
    f.write('hello')

with fs.open('DEMO.md', 'r') as f:
    print(f.read())
print(fs.info('DEMO.md'))

"""
Read and Write from/to Dropbox FileSystem
"""
import os
from dropboxdrivefs import DropboxDriveFileSystem
fs = DropboxDriveFileSystem(token=os.environ['DROPBOX_TOKEN'])
assert isinstance(fs, AbstractFileSystem)
with fs.open('/test_dropbox/DEMO.md', 'w') as f:
    f.write('hello')

with fs.open('/test_dropbox/DEMO.md', 'r') as f:
    print(f.read())
print(fs.info('/test_dropbox/DEMO.md'))
"""
Read and Write from/to Dropbox FileSystem's /test_dropbox directory
"""
dfs = DirFileSystem('/test_dropbox', fs)
with dfs.open('DEMO2.md', 'w') as f:
    f.write('hello')

with dfs.open('DEMO2.md', 'r') as f:
    print(f.read())
print(dfs.info('DEMO2.md'))

