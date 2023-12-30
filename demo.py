from fsspec.implementations.local import LocalFileSystem

"""
Read and Write from/to Local FileSystem
"""
fs = LocalFileSystem()

f = fs.open('README.md', 'r')
print(f.read())

f = fs.open('NEW.md', 'w')
f.write('hello')

print(fs.info('README.md'))

"""
Read and Write from/to Dropbox FileSystem
"""
import os
import dropboxdrivefs as dbx
fs = dbx.DropboxDriveFileSystem(token=os.environ['DROPBOX_TOKEN'])
# fs.mkdir("/test_dropbox")

fs.put_file("NEW.md", "/Data/test_dropbox/NEW.txt")
