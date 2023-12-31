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
assert fs.exists('DEMO.md')
fs.rm('DEMO.md')
assert not fs.exists('DEMO.md')
print('root_path:', fs.path)
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

assert dfs.exists('DEMO2.md')

"""
Upload PyArrow
"""
import pandas as pd 
table = pd.read_parquet('../dropbox_duckdb_playground/examples/data/subgraph/output/has_requirement.parquet')
import io
buff = io.BytesIO()
table.to_parquet(buff)
buff.seek(0)
memory_data = buff.getbuffer()
print(len(memory_data))

# fs.upload('../dropbox_duckdb_playground/examples/data/subgraph/output/package.parquet', '/test.parquet')

# table['partition'] = table.index.map(lambda x: str(x % 100))
# tables = [x[1] for x in table.groupby('partition')]
# for i, t in enumerate(tables):
#     with fs.open(f'/test.{i}.parquet', 'wb') as f:
#         t.to_parquet(f)
#     print('upload', i)
