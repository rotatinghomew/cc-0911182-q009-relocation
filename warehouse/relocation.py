"""跨部署的湖表快照迁移包。

迁移包只携带明确选择的表快照及其内容寻址列式对象，不复制整个湖目录。
- 构建端按快照选择打包，已在接收端存在的对象可以只声明为依赖，不重复携带。
- 接收端逐对象校验 SHA-256，依赖缺失或损坏、清单被篡改都不会写入目录。
- 对象先原子落盘，目录插入与正式接口回读在同一事务内提交，任何失败都
  回滚并清理新对象，保留接收端原有可用目录，不产生可被正式读取的残缺
  快照。
- 同内容对象天然复用同一路径；表身份（table_id/name/source_identity）
  与来源不一致时拒绝导入，不按名称覆盖已有表。
"""

import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path

from .checks import text
from .lake import object_path, read_snapshot
from .storage import canonical, transaction

PACKAGE_FORMAT = 'lake-snapshot-relocation/1'
MANIFEST_NAME = 'manifest.json'
DIGEST_NAME = 'manifest.sha256'
REVISION_PATTERN = re.compile(r'[A-Za-z0-9_-]{1,64}')
HASH_PATTERN = re.compile(r'[a-f0-9]{64}')
MANIFEST_ROOT_KEYS = {'format', 'revision', 'created_at', 'tables', 'snapshots',
                      'included_objects', 'required_objects'}


def valid_revision(value):
    if not isinstance(value, str) or not REVISION_PATTERN.fullmatch(value):
        raise ValueError('迁移包标识非法')
    return value


def valid_hash(value):
    if not isinstance(value, str) or not HASH_PATTERN.fullmatch(value):
        raise ValueError('对象摘要非法')
    return value


def write_inventory(path, connection):
    """导出接收端当前已登记对象，供发送端做增量选择。"""
    hashes = [row[0] for row in connection.execute(
        'SELECT object_hash FROM objects ORDER BY object_hash').fetchall()]
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(canonical({'format': 'lake-object-inventory/1', 'objects': hashes}),
                      encoding='utf-8')
    return hashes


def read_inventory(path):
    """读取他部署提供的对象清单，非法文件直接拒绝。"""
    payload = json.loads(Path(path).read_text(encoding='utf-8'))
    if not isinstance(payload, dict) or set(payload) != {'format', 'objects'}:
        raise ValueError('对象清单结构非法')
    if payload['format'] != 'lake-object-inventory/1' or not isinstance(payload['objects'], list):
        raise ValueError('对象清单结构非法')
    return set(valid_hash(value) for value in payload['objects'])


def build_package(connection, lake_root, destination, revision, snapshot_ids,
                  created_at, present_objects=None):
    """构建一个只包含选定快照的迁移包。

    present_objects 为接收端清单中已有的对象摘要：这些对象不放入包内，
    但会在 required_objects 中声明为先前内容依赖。
    """
    valid_revision(revision)
    text(created_at)
    if not isinstance(snapshot_ids, list) or not snapshot_ids:
        raise ValueError('至少选择一个快照')
    if len(set(snapshot_ids)) != len(snapshot_ids):
        raise ValueError('快照选择存在重复')
    present = set(present_objects or ())
    for value in present:
        valid_hash(value)
    lake_root = Path(lake_root)

    tables = {}
    snapshots = []
    referenced = set()
    for snapshot_id in snapshot_ids:
        row = connection.execute(
            'SELECT s.table_id,s.object_hash,s.row_count,t.name,t.source_identity '
            'FROM snapshots s JOIN lake_tables t ON t.table_id=s.table_id '
            'WHERE s.snapshot_id=?', [snapshot_id]).fetchone()
        if row is None:
            raise ValueError('快照不存在：' + str(snapshot_id))
        table_id, object_hash, row_count, name, source_identity = row
        valid_hash(object_hash)
        entry = tables.get(table_id)
        table_identity = {'name': name, 'source_identity': source_identity}
        if entry is None:
            tables[table_id] = table_identity
        elif entry != table_identity:
            raise ValueError('表身份声明不一致')
        source = object_path(lake_root, object_hash)
        if not source.is_file() or hashlib.sha256(source.read_bytes()).hexdigest() != object_hash:
            raise ValueError('快照对象缺失或损坏：' + snapshot_id)
        snapshots.append({'snapshot_id': snapshot_id, 'table_id': table_id,
                          'object_hash': object_hash, 'row_count': row_count})
        referenced.add(object_hash)

    sizes = dict(connection.execute('SELECT object_hash,byte_size FROM objects').fetchall())
    included = sorted(referenced - present)
    required = sorted(referenced & present)
    included_objects = []
    for object_hash in included:
        if object_hash not in sizes:
            raise ValueError('对象缺少目录登记：' + object_hash)
        included_objects.append({'object_hash': object_hash, 'byte_size': sizes[object_hash],
                                 'path': 'objects/' + object_hash[:2] + '/' + object_hash + '.parquet'})

    manifest = {'format': PACKAGE_FORMAT, 'revision': revision, 'created_at': created_at,
                'tables': tables, 'snapshots': snapshots,
                'included_objects': included_objects, 'required_objects': required}

    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / revision
    if target.exists():
        raise FileExistsError('迁移包已存在')
    stage = Path(tempfile.mkdtemp(prefix='.package-', dir=destination))
    try:
        for entry in included_objects:
            source = object_path(lake_root, entry['object_hash'])
            output = stage / entry['path']
            output.parent.mkdir(parents=True, exist_ok=True)
            data = source.read_bytes()
            if len(data) != entry['byte_size']:
                raise ValueError('对象大小与目录登记不符')
            with output.open('wb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
        (stage / MANIFEST_NAME).write_text(canonical(manifest), encoding='utf-8')
        manifest_hash = hashlib.sha256(canonical(manifest).encode('utf-8')).hexdigest()
        with (stage / DIGEST_NAME).open('w', encoding='utf-8') as stream:
            stream.write(manifest_hash + '  ' + MANIFEST_NAME + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.rename(stage, target)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    return manifest


def _load_manifest(destination, revision):
    valid_revision(revision)
    root = Path(destination) / revision
    try:
        root.resolve(strict=True).relative_to(Path(destination).resolve(strict=True))
    except (OSError, ValueError):
        raise ValueError('迁移包位置非法')
    manifest_file = root / MANIFEST_NAME
    digest_file = root / DIGEST_NAME
    manifest_bytes = manifest_file.read_bytes()
    digest_line = digest_file.read_text(encoding='utf-8').strip().split()
    if len(digest_line) != 2 or digest_line[1] != MANIFEST_NAME:
        raise ValueError('清单摘要声明非法')
    if digest_line[0] != hashlib.sha256(manifest_bytes).hexdigest():
        raise ValueError('迁移包清单与摘要不符')
    manifest = json.loads(manifest_bytes.decode('utf-8'))
    _validate_manifest(manifest, revision)
    return root, manifest


def _validate_manifest(manifest, revision):
    if not isinstance(manifest, dict) or set(manifest) != MANIFEST_ROOT_KEYS:
        raise ValueError('迁移包清单结构非法')
    if manifest['format'] != PACKAGE_FORMAT or manifest['revision'] != revision:
        raise ValueError('迁移包格式或标识不符')
    if not isinstance(manifest['created_at'], str) or not manifest['created_at'].strip():
        raise ValueError('迁移包时间声明非法')
    tables, snapshots = manifest['tables'], manifest['snapshots']
    included, required = manifest['included_objects'], manifest['required_objects']
    if not isinstance(tables, dict) or not tables or not isinstance(snapshots, list) or not snapshots:
        raise ValueError('迁移包内容为空或结构非法')
    for table_id, identity in tables.items():
        text(table_id)
        if not isinstance(identity, dict) or set(identity) != {'name', 'source_identity'}:
            raise ValueError('表身份声明非法：' + table_id)
        text(identity['name'])
        text(identity['source_identity'])
    snapshot_ids, referenced = set(), set()
    for entry in snapshots:
        if not isinstance(entry, dict) or set(entry) != {'snapshot_id', 'table_id', 'object_hash', 'row_count'}:
            raise ValueError('快照声明非法')
        text(entry['snapshot_id'])
        if entry['snapshot_id'] in snapshot_ids:
            raise ValueError('快照声明重复')
        snapshot_ids.add(entry['snapshot_id'])
        if entry['table_id'] not in tables:
            raise ValueError('快照引用了未声明的表')
        valid_hash(entry['object_hash'])
        if type(entry['row_count']) is not int or entry['row_count'] < 0:
            raise ValueError('快照行数声明非法')
        referenced.add(entry['object_hash'])
    if not isinstance(included, list) or not isinstance(required, list):
        raise ValueError('对象清单结构非法')
    included_hashes, declared_paths = set(), set()
    for entry in included:
        if not isinstance(entry, dict) or set(entry) != {'object_hash', 'byte_size', 'path'}:
            raise ValueError('随包对象声明非法')
        object_hash = valid_hash(entry['object_hash'])
        if object_hash in included_hashes:
            raise ValueError('随包对象声明重复')
        included_hashes.add(object_hash)
        if type(entry['byte_size']) is not int or entry['byte_size'] < 0:
            raise ValueError('对象大小声明非法')
        expected = 'objects/' + object_hash[:2] + '/' + object_hash + '.parquet'
        if entry['path'] != expected:
            raise ValueError('对象路径声明非法')
        declared_paths.add(expected)
    required_hashes = set()
    for value in required:
        required_hashes.add(valid_hash(value))
    if included_hashes & required_hashes:
        raise ValueError('同一对象不能既随包又声明为依赖')
    if included_hashes | required_hashes != referenced:
        raise ValueError('对象声明与快照引用不一致')
    return tables, snapshots, included, included_hashes, required_hashes, declared_paths


def _scan_package_files(root, declared_paths):
    actual_files = set()
    for current in root.rglob('*'):
        if current.is_symlink():
            raise ValueError('迁移包不允许符号链接')
        if current.is_file():
            relative = current.relative_to(root).as_posix()
            if relative not in (MANIFEST_NAME, DIGEST_NAME):
                actual_files.add(relative)
    if actual_files != declared_paths:
        raise ValueError('迁移包文件与清单不一致')


def _verify_included_objects(root, included):
    staged_data = []
    for entry in included:
        path = root / entry['path']
        if path.is_symlink():
            raise ValueError('迁移包不允许符号链接')
        data = path.read_bytes()
        if len(data) != entry['byte_size']:
            raise ValueError('随包对象大小不符：' + entry['object_hash'])
        if hashlib.sha256(data).hexdigest() != entry['object_hash']:
            raise ValueError('随包对象内容损坏：' + entry['object_hash'])
        staged_data.append((entry, data))
    return staged_data


def describe_package(destination, revision):
    """在不写入任何目录的情况下核对并描述迁移包范围。"""
    root, manifest = _load_manifest(destination, revision)
    _, _, included, _, _, declared_paths = _validate_manifest(manifest, revision)
    _scan_package_files(root, declared_paths)
    _verify_included_objects(root, included)
    return {
        'revision': revision,
        'table_ids': sorted(manifest['tables']),
        'snapshot_ids': [entry['snapshot_id'] for entry in manifest['snapshots']],
        'included_objects': [entry['object_hash'] for entry in included],
        'required_objects': list(manifest['required_objects']),
        'verified': True,
    }


def import_package(connection, lake_root, destination, revision):
    """校验并导入迁移包，失败时不改动接收端原有目录。"""
    root, manifest = _load_manifest(destination, revision)
    (tables, snapshots, included, included_hashes,
     required_hashes, declared_paths) = _validate_manifest(manifest, revision)
    _scan_package_files(root, declared_paths)

    staged = []
    for entry, data in _verify_included_objects(root, included):
        target = object_path(lake_root, entry['object_hash'])
        if target.exists():
            if target.is_symlink() or hashlib.sha256(target.read_bytes()).hexdigest() != entry['object_hash']:
                raise ValueError('目标位置已有不同内容：' + entry['object_hash'])
        else:
            staged.append((target, data))

    lake_root = Path(lake_root)
    for table_id, identity in tables.items():
        existing = connection.execute(
            'SELECT name,source_identity FROM lake_tables WHERE table_id=?',
            [table_id]).fetchone()
        if existing is not None and tuple(existing) != (identity['name'], identity['source_identity']):
            raise ValueError('表身份冲突，拒绝按名称或标识覆盖：' + table_id)
        colliding = connection.execute(
            'SELECT table_id FROM lake_tables WHERE name=? AND table_id<>?',
            [identity['name'], table_id]).fetchall()
        if colliding:
            raise ValueError('接收端已有同名不同来源的表，拒绝合并：' + identity['name'])
    for entry in snapshots:
        existing = connection.execute(
            'SELECT table_id,object_hash,row_count FROM snapshots WHERE snapshot_id=?',
            [entry['snapshot_id']]).fetchone()
        if existing is not None and tuple(existing) != (
                entry['table_id'], entry['object_hash'], entry['row_count']):
            raise ValueError('快照标识冲突：' + entry['snapshot_id'])
    for entry in included:
        existing = connection.execute(
            'SELECT byte_size FROM objects WHERE object_hash=?',
            [entry['object_hash']]).fetchone()
        if existing is not None and existing[0] != entry['byte_size']:
            raise ValueError('对象登记大小冲突：' + entry['object_hash'])

    # 依赖对象必须同时具备目录登记与完好内容，对象存在但缺少目录成员
    # 不能算作可用的先前内容。
    for object_hash in required_hashes:
        row = connection.execute('SELECT byte_size FROM objects WHERE object_hash=?',
                                 [object_hash]).fetchone()
        if row is None:
            raise ValueError('缺少迁移包依赖的先前对象：' + object_hash)
        target = object_path(lake_root, object_hash)
        if not target.is_file() or hashlib.sha256(target.read_bytes()).hexdigest() != object_hash:
            raise ValueError('依赖的先前对象缺失或损坏：' + object_hash)

    placed = []
    try:
        for target, data in staged:
            target.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary = tempfile.mkstemp(prefix='.incoming-', dir=target.parent)
            try:
                with os.fdopen(descriptor, 'wb') as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.rename(temporary, target)
            finally:
                Path(temporary).unlink(missing_ok=True)
            placed.append(target)

        with transaction(connection):
            for table_id, identity in tables.items():
                connection.execute(
                    'INSERT INTO lake_tables VALUES (?,?,?) ON CONFLICT DO NOTHING',
                    [table_id, identity['name'], identity['source_identity']])
            for entry in included:
                connection.execute(
                    'INSERT INTO objects VALUES (?,?) ON CONFLICT DO NOTHING',
                    [entry['object_hash'], entry['byte_size']])
            for entry in snapshots:
                connection.execute(
                    'INSERT INTO snapshots VALUES (?,?,?,?) ON CONFLICT DO NOTHING',
                    [entry['snapshot_id'], entry['table_id'],
                     entry['object_hash'], entry['row_count']])

            # 目录成员在同一事务内经正式读取接口回读验证；行数、对象
            # 内容或依赖有任何不符都会随事务一起回滚，不留下可被正式
            # 读取的残缺快照。
            for entry in snapshots:
                rows = read_snapshot(connection, lake_root, entry['snapshot_id'])
                if len(rows) != entry['row_count']:
                    raise ValueError('导入快照行数不符：' + entry['snapshot_id'])
    except BaseException:
        for target in placed:
            target.unlink(missing_ok=True)
        raise

    return {
        'revision': revision,
        'imported_snapshots': [entry['snapshot_id'] for entry in snapshots],
        'objects_added': len(placed),
        'objects_reused': len(included_hashes) - len(placed),
        'dependencies_satisfied': sorted(required_hashes),
        'verified': True,
    }
