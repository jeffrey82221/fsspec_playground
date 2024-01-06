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

"""
Dropbox Setup
"""
import os
from dropboxdrivefs import DropboxDriveFileSystem
fs = DropboxDriveFileSystem(token=os.environ['DROPBOX_TOKEN'])
if fs.exists('/test_dropbox'):
    fs.rm('/test_dropbox')
if fs.exists('/test_dropbox_copy'):
    fs.rm('/test_dropbox_copy')
fs.mkdir('/test_dropbox')
assert isinstance(fs, AbstractFileSystem)
with fs.open('/test_dropbox/DEMO.md', 'w') as f:
    f.write('hello')

print(fs.info('/test_dropbox/DEMO.md'))
print(fs.info('/test_dropbox'))
print(fs.isdir('/test_dropbox/DEMO.md'))
"""
New copy method
"""
assert fs.exists('/test_dropbox/DEMO.md')
# fs.cp_file('/test_dropbox/DEMO.md', '/test_dropbox/DEMO_COPY2.md')
# -> NotImplementedError
"""
Copy file
"""
import dropbox
client = dropbox.Dropbox(os.environ['DROPBOX_TOKEN'])
client.files_copy('/test_dropbox/DEMO.md', '/test_dropbox/DEMO_COPY.md')
"""
Copy folder
"""
import dropbox
client = dropbox.Dropbox(os.environ['DROPBOX_TOKEN'])
client.files_copy('/test_dropbox', '/test_dropbox_copy')