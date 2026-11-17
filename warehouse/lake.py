import hashlib
import os
import re
import tempfile
from pathlib import Path
from .storage import records,transaction


def object_path(root,signature):
    if not re.fullmatch('[a-f0-9]{64}',signature):
        raise ValueError('对象摘要无效')
    return Path(root)/'objects'/signature[:2]/(signature+'.parquet')


def create_snapshot(connection,root,resource,table_id,snapshot_id,source_identity):
    info=connection.execute('SELECT name FROM resources WHERE resource_id=?',[resource]).fetchone()
    if info is None:
        raise ValueError('资源集合不存在')
    existing=connection.execute('SELECT name,source_identity FROM lake_tables WHERE table_id=?',[table_id]).fetchone()
    if existing and existing!=(info[0],source_identity):
        raise ValueError('表身份与已登记对象不一致')
    if connection.execute('SELECT 1 FROM snapshots WHERE snapshot_id=?',[snapshot_id]).fetchone():
        raise ValueError('快照标识已存在')
    root=Path(root)
    root.mkdir(parents=True,exist_ok=True)
    descriptor,temporary=tempfile.mkstemp(suffix='.parquet',dir=root)
    os.close(descriptor)
    Path(temporary).unlink()
    try:
        escaped=temporary.replace("'","''")
        connection.execute("COPY (SELECT content_id,title,year FROM resource_rows WHERE resource_id=? ORDER BY row_id) TO '"+escaped+"' (FORMAT PARQUET,COMPRESSION ZSTD)",[resource])
        data=Path(temporary).read_bytes()
        signature=hashlib.sha256(data).hexdigest()
        destination=object_path(root,signature)
        destination.parent.mkdir(parents=True,exist_ok=True)
        if destination.exists():
            if destination.read_bytes()!=data:
                raise ValueError('已有对象内容损坏')
        else:
            os.rename(temporary,destination)
        count=connection.execute('SELECT count(*) FROM resource_rows WHERE resource_id=?',[resource]).fetchone()[0]
        with transaction(connection):
            connection.execute('INSERT INTO lake_tables VALUES (?,?,?) ON CONFLICT DO NOTHING',[table_id,info[0],source_identity])
            connection.execute('INSERT INTO objects VALUES (?,?) ON CONFLICT DO NOTHING',[signature,len(data)])
            connection.execute('INSERT INTO snapshots VALUES (?,?,?,?)',[snapshot_id,table_id,signature,count])
        return {'snapshot_id':snapshot_id,'object_hash':signature,'rows':count}
    finally:
        Path(temporary).unlink(missing_ok=True)


def read_snapshot(connection,root,snapshot):
    row=connection.execute('SELECT object_hash,row_count FROM snapshots WHERE snapshot_id=?',[snapshot]).fetchone()
    if row is None:
        raise ValueError('快照不存在')
    path=object_path(root,row[0])
    data=path.read_bytes()
    if hashlib.sha256(data).hexdigest()!=row[0]:
        raise ValueError('快照对象损坏')
    result=records(connection,'SELECT * FROM read_parquet(?) ORDER BY content_id',[str(path)])
    if len(result)!=row[1]:
        raise ValueError('快照行数不符')
    return result
