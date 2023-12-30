from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.dirfs import DirFileSystem
"""
Read and Write from/to Local FileSystem
"""
fs = LocalFileSystem()
f = fs.open('DEMO.md', 'w')
f.write('hello')

f = fs.open('DEMO.md', 'r')
print(f.read())
print(fs.info('DEMO.md'))
"""
Change root_path of a filesystem
"""
fs = DirFileSystem('./data')
f = fs.open('DEMO.md', 'w')
f.write('hello')

f = fs.open('DEMO.md', 'r')
print(f.read())
print(fs.info('DEMO.md'))

"""
Read and Write from/to Dropbox FileSystem
"""
import os
import dropboxdrivefs as dbx
fs = dbx.DropboxDriveFileSystem(token=os.environ['DROPBOX_TOKEN'])
fs.put_file("NEW.md", "/Data/test_dropbox/NEW.txt")
