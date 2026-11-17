import hashlib
import os
import re
from pathlib import Path
from .lake import object_path,read_snapshot


def export_snapshot(connection,lake_root,snapshot,export_root,export_id):
    if not re.fullmatch('[A-Za-z0-9_-]+',export_id):
        raise ValueError('本地导出标识无效')
    read_snapshot(connection,lake_root,snapshot)
    signature=connection.execute('SELECT object_hash FROM snapshots WHERE snapshot_id=?',[snapshot]).fetchone()[0]
    filename=export_id+'.parquet'
    target=Path(export_root)/filename
    previous=connection.execute('SELECT snapshot_id,filename,object_hash FROM snapshot_exports WHERE export_id=?',[export_id]).fetchone()
    if previous:
        if previous!=(snapshot,filename,signature):
            raise ValueError('导出标识已使用')
        if not target.is_file() or hashlib.sha256(target.read_bytes()).hexdigest()!=signature:
            raise ValueError('已登记的本地导出缺失或损坏')
        return str(target)
    target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists():
        raise FileExistsError('导出位置已存在未登记文件')
    data=object_path(lake_root,signature).read_bytes()
    with target.open('xb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    connection.execute('INSERT INTO snapshot_exports VALUES (?,?,?,?)',[export_id,snapshot,filename,signature])
    return str(target)
