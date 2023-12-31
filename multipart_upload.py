import pandas as pd
import io
table = pd.read_parquet('../dropbox_duckdb_playground/examples/data/subgraph/output/has_requirement.parquet')
buff = io.BytesIO()
table.to_parquet(buff)
buff.seek(0)
memory_data = buff.getbuffer()
print(len(memory_data))
def split_bytes(memory_data, chunk_size=1000):
    for i in range(len(memory_data)//chunk_size + 1):
        yield memory_data[i * chunk_size: (i+1) * chunk_size]

chunk_size = 2000000
chunks = [chunk for chunk in split_bytes(memory_data, chunk_size=chunk_size)]
print('number of chunks:', len(chunks))
size = sum([len(c) for c in chunks])
print('size:', size)

import os
from dropboxdrivefs import DropboxDriveFileSystem
from fsspec.implementations.dirfs import DirFileSystem
from concurrent.futures import ThreadPoolExecutor
fs = DropboxDriveFileSystem(token=os.environ['DROPBOX_TOKEN'])
fs.rm('/chunks')
fs.mkdir('/chunks')
dfs = DirFileSystem('/chunks', fs)
def partial_upload(item):
    index = item[0]
    chunk = item[1]
    with dfs.open(f'{index}.parquet', 'wb') as f:
        f.write(chunk)
    print('upload', index, 'size:', len(chunk))

with ThreadPoolExecutor(max_workers=8) as executor:
    executor.map(partial_upload, enumerate(chunks))


def partial_download(index):
    with dfs.open(f'{index}.parquet', 'rb') as f:
        result = f.read()
        print('download', index, 'size:', len(result))
    return index, result

files = [f for f in fs.listdir('/chunks') if f['name'] != '/chunks' and f['name'].startswith('/chunks')]
print('files', files)
remote_chunk_count = len(files)
print('remote_chunk_count:', remote_chunk_count)
with ThreadPoolExecutor(max_workers=8) as executor:
    results = list(executor.map(partial_download, range(remote_chunk_count)))
print('size:', sum([len(x[1]) for x in results]))
output_bytes = b''.join(list([x[1] for x in results]))
print('output_bytes', len(output_bytes))
buff = io.BytesIO(output_bytes)
buff.seek(0)
download_table = pd.read_parquet(buff)
fs.rm('/chunks')
from pandas.testing import assert_frame_equal
assert_frame_equal(table, download_table)