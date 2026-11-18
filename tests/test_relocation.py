import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from warehouse.lake import create_snapshot, object_path, read_snapshot
from warehouse.relocation import (DIGEST_NAME, MANIFEST_NAME, build_package,
                                  describe_package, import_package,
                                  read_inventory, write_inventory)
from warehouse.storage import canonical, open_store

CREATED_AT = '2026-11-18T00:00:00+00:00'


class RelocationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.src_db = self.root / 'src.duckdb'
        self.dst_db = self.root / 'dst.duckdb'
        self.src_lake = self.root / 'src-lake'
        self.dst_lake = self.root / 'dst-lake'
        self.packages = self.root / 'packages'
        self.source = open_store(self.src_db)
        self.target = open_store(self.dst_db)
        self._build_source()

    def tearDown(self):
        self.source.close()
        self.target.close()
        self.temp.cleanup()

    def _build_source(self):
        self.source.execute(
            "INSERT INTO resources VALUES ('fujian','福建文化记忆','zh'),"
            "('zhejiang','浙江文化资源','zh'),('jiangxi','江西文化资源','zh')")
        self.source.execute(
            "INSERT INTO resource_rows VALUES "
            "('r1','fujian','c1','南音',2025),"
            "('r2','fujian','c2','海丝船政',2026),"
            "('z1','zhejiang','z1','越剧',1900),"
            "('z2','zhejiang','z2','龙泉青瓷',1500)")
        # 江西资源与福建行内容相同，产生共享同一内容对象的快照
        self.source.execute(
            "INSERT INTO resource_rows VALUES "
            "('j1','jiangxi','c1','南音',2025),"
            "('j2','jiangxi','c2','海丝船政',2026)")
        self.snap_fj = create_snapshot(self.source, self.src_lake, 'fujian',
                                       'table-fj', 'snap-fj', 'office-a')
        self.snap_zj = create_snapshot(self.source, self.src_lake, 'zhejiang',
                                       'table-zj', 'snap-zj', 'office-a')
        self.snap_jx = create_snapshot(self.source, self.src_lake, 'jiangxi',
                                       'table-jx', 'snap-jx', 'office-b')
        self.fj_hash = self.snap_fj['object_hash']
        self.zj_hash = self.snap_zj['object_hash']
        self.assertEqual(self.fj_hash, self.snap_jx['object_hash'])
        self.assertNotEqual(self.fj_hash, self.zj_hash)
        # 源湖对象按内容寻址，共享对象只存一份
        self.assertEqual(len(list((self.src_lake / 'objects').rglob('*.parquet'))), 2)

    def package_path(self, revision):
        return self.packages / revision

    def manifest(self, revision):
        return json.loads((self.package_path(revision) / MANIFEST_NAME).read_text())

    def test_full_package_roundtrip_reads_identical_data(self):
        manifest = build_package(self.source, self.src_lake, self.packages, 'rel1',
                                 ['snap-fj', 'snap-zj'], CREATED_AT)
        self.assertEqual(sorted(manifest['tables']), ['table-fj', 'table-zj'])
        self.assertEqual(len(manifest['included_objects']), 2)
        report = import_package(self.target, self.dst_lake, self.packages, 'rel1')
        self.assertEqual(sorted(report['imported_snapshots']), ['snap-fj', 'snap-zj'])
        self.assertEqual(report['objects_added'], 2)
        self.assertEqual(
            read_snapshot(self.target, self.dst_lake, 'snap-fj'),
            read_snapshot(self.source, self.src_lake, 'snap-fj'))
        self.assertEqual(
            read_snapshot(self.target, self.dst_lake, 'snap-zj'),
            read_snapshot(self.source, self.src_lake, 'snap-zj'))
        identity = self.target.execute(
            'SELECT name,source_identity FROM lake_tables WHERE table_id=?',
            ['table-fj']).fetchone()
        self.assertEqual(identity, ('福建文化记忆', 'office-a'))

    def test_package_contains_only_selected_snapshots(self):
        build_package(self.source, self.src_lake, self.packages, 'rel-fj',
                      ['snap-fj'], CREATED_AT)
        manifest = self.manifest('rel-fj')
        self.assertEqual([entry['snapshot_id'] for entry in manifest['snapshots']], ['snap-fj'])
        self.assertEqual(list(manifest['tables']), ['table-fj'])
        self.assertEqual([entry['object_hash'] for entry in manifest['included_objects']],
                         [self.fj_hash])
        # 未选择的浙江对象和江西表均不得进入交付包
        all_files = {path.relative_to(self.package_path('rel-fj')).as_posix()
                     for path in self.package_path('rel-fj').rglob('*') if path.is_file()}
        self.assertNotIn('objects/' + self.zj_hash[:2] + '/' + self.zj_hash + '.parquet',
                         all_files)
        description = describe_package(self.packages, 'rel-fj')
        self.assertEqual(description['snapshot_ids'], ['snap-fj'])
        self.assertEqual(description['table_ids'], ['table-fj'])
        self.assertTrue(description['verified'])

    def test_incremental_package_skips_objects_receiver_already_has(self):
        # 先迁移福建
        build_package(self.source, self.src_lake, self.packages, 'rel-base',
                      ['snap-fj'], CREATED_AT)
        import_package(self.target, self.dst_lake, self.packages, 'rel-base')
        # 接收端导出对象清单；江西快照与福建共享对象，增量包只声明依赖
        inventory_file = self.root / 'inventory.json'
        hashes = write_inventory(inventory_file, self.target)
        self.assertEqual(hashes, [self.fj_hash])
        self.assertEqual(read_inventory(inventory_file), {self.fj_hash})
        manifest = build_package(self.source, self.src_lake, self.packages, 'rel-add',
                                 ['snap-jx'], CREATED_AT, read_inventory(inventory_file))
        self.assertEqual(manifest['included_objects'], [])
        self.assertEqual(manifest['required_objects'], [self.fj_hash])
        self.assertFalse(any((self.package_path('rel-add') / 'objects').rglob('*')))
        report = import_package(self.target, self.dst_lake, self.packages, 'rel-add')
        self.assertEqual(report['objects_added'], 0)
        self.assertEqual(report['dependencies_satisfied'], [self.fj_hash])
        self.assertEqual(
            read_snapshot(self.target, self.dst_lake, 'snap-jx'),
            read_snapshot(self.source, self.src_lake, 'snap-jx'))

    def test_missing_dependency_is_rejected_and_leaves_no_half_table(self):
        # 没有接收端清单的情况下手工构造一个声明依赖的增量包
        build_package(self.source, self.src_lake, self.packages, 'rel-base',
                      ['snap-fj'], CREATED_AT)
        manifest = build_package(self.source, self.src_lake, self.packages, 'rel-zj',
                                 ['snap-zj'], CREATED_AT)
        manifest['required_objects'] = [self.fj_hash]
        manifest['included_objects'] = [entry for entry in manifest['included_objects']
                                        if entry['object_hash'] != self.fj_hash]
        self._rewrite_manifest('rel-zj', manifest)
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-zj')
        self._assert_target_pristine()

    def test_dependency_without_catalog_member_is_not_complete(self):
        build_package(self.source, self.src_lake, self.packages, 'rel-base',
                      ['snap-fj'], CREATED_AT)
        import_package(self.target, self.dst_lake, self.packages, 'rel-base')
        # 目录中移除对象成员，但对象文件仍在磁盘上
        self.target.execute('DELETE FROM objects WHERE object_hash=?', [self.fj_hash])
        build_package(self.source, self.src_lake, self.packages, 'rel-jx',
                      ['snap-jx'], CREATED_AT, {self.fj_hash})
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-jx')
        self.assertEqual(
            self.target.execute('SELECT count(*) FROM snapshots').fetchone()[0], 1)
        self.assertEqual(
            self.target.execute('SELECT count(*) FROM lake_tables').fetchone()[0], 1)

    def test_corrupt_included_object_is_rejected(self):
        build_package(self.source, self.src_lake, self.packages, 'rel-bad',
                      ['snap-fj'], CREATED_AT)
        object_file = object_path(self.package_path('rel-bad'), self.fj_hash)
        object_file.write_bytes(object_file.read_bytes() + b'tampered')
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-bad')
        self._assert_target_pristine()
        # 对象在落库前被拒绝，接收端对象目录没有任何残留
        self.assertFalse(self.dst_lake.exists() and
                         list((self.dst_lake / 'objects').rglob('*.parquet')))

    def test_manifest_tampering_is_detected(self):
        build_package(self.source, self.src_lake, self.packages, 'rel-tamper',
                      ['snap-fj'], CREATED_AT)
        manifest = self.manifest('rel-tamper')
        manifest['revision'] = 'forged'
        self._rewrite_manifest('rel-tamper', manifest)
        with self.assertRaises(ValueError):
            describe_package(self.packages, 'rel-tamper')
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-tamper')
        self._assert_target_pristine()

    def test_tampered_row_count_rolls_back_catalog_and_objects(self):
        # 行数声明被改大时，事务内回读会失败，目录与对象都不得残留
        build_package(self.source, self.src_lake, self.packages, 'rel-rows',
                      ['snap-fj'], CREATED_AT)
        manifest = self.manifest('rel-rows')
        manifest['snapshots'][0]['row_count'] = 999
        self._rewrite_manifest('rel-rows', manifest)
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-rows')
        self._assert_target_pristine()
        self.assertFalse(any(self.dst_lake.rglob('*.parquet')))

    def test_extra_or_missing_package_files_are_rejected(self):
        build_package(self.source, self.src_lake, self.packages, 'rel-files',
                      ['snap-fj'], CREATED_AT)
        stray = self.package_path('rel-files') / 'objects' / 'stray.parquet'
        stray.parent.mkdir(parents=True, exist_ok=True)
        stray.write_bytes(b'unlisted')
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-files')
        stray.unlink()
        listed = object_path(self.package_path('rel-files'), self.fj_hash)
        listed.unlink()
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-files')
        self._assert_target_pristine()

    def test_same_name_different_source_table_is_not_overwritten(self):
        self.target.execute("INSERT INTO lake_tables VALUES ('table-local','福建文化记忆','office-z')")
        build_package(self.source, self.src_lake, self.packages, 'rel-name',
                      ['snap-fj'], CREATED_AT)
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-name')
        # 原表保持不动，源表没有被按同名合并
        self.assertEqual(
            self.target.execute("SELECT source_identity FROM lake_tables WHERE table_id='table-local'")
            .fetchone()[0], 'office-z')
        self.assertIsNone(
            self.target.execute("SELECT 1 FROM lake_tables WHERE table_id='table-fj'").fetchone())

    def test_same_table_id_with_different_identity_is_rejected(self):
        self.target.execute("INSERT INTO lake_tables VALUES ('table-fj','别的名字','office-z')")
        build_package(self.source, self.src_lake, self.packages, 'rel-id',
                      ['snap-fj'], CREATED_AT)
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-id')
        self.assertEqual(
            self.target.execute("SELECT name FROM lake_tables WHERE table_id='table-fj'")
            .fetchone()[0], '别的名字')

    def test_snapshot_id_collision_is_rejected(self):
        build_package(self.source, self.src_lake, self.packages, 'rel-snap',
                      ['snap-fj'], CREATED_AT)
        import_package(self.target, self.dst_lake, self.packages, 'rel-snap')
        # 伪造一个复用 snap-fj 标识但指向不同对象的包
        manifest = self.manifest('rel-snap')
        manifest['snapshots'][0]['object_hash'] = self.zj_hash
        self._rewrite_manifest('rel-snap', manifest)
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-snap')
        self.assertEqual(
            self.target.execute('SELECT object_hash FROM snapshots WHERE snapshot_id=?',
                                ['snap-fj']).fetchone()[0], self.fj_hash)

    def test_failed_import_retry_keeps_working_catalog(self):
        # 先成功导入浙江，建立接收端可用目录
        build_package(self.source, self.src_lake, self.packages, 'rel-ok',
                      ['snap-zj'], CREATED_AT)
        import_package(self.target, self.dst_lake, self.packages, 'rel-ok')
        # 第二个包损坏，导入必须失败且不影响已可查询的浙江快照
        build_package(self.source, self.src_lake, self.packages, 'rel-fail',
                      ['snap-fj'], CREATED_AT)
        object_file = object_path(self.package_path('rel-fail'), self.fj_hash)
        object_file.write_bytes(b'broken')
        with self.assertRaises(ValueError):
            import_package(self.target, self.dst_lake, self.packages, 'rel-fail')
        self.assertEqual(
            len(read_snapshot(self.target, self.dst_lake, 'snap-zj')), 2)
        # 修好后重试，原目录仍在，新快照接续导入成功
        shutil.copy(object_path(self.src_lake, self.fj_hash), object_file)
        report = import_package(self.target, self.dst_lake, self.packages, 'rel-fail')
        self.assertTrue(report['verified'])
        self.assertEqual(
            len(read_snapshot(self.target, self.dst_lake, 'snap-fj')), 2)

    def test_reimport_is_idempotent(self):
        build_package(self.source, self.src_lake, self.packages, 'rel-idem',
                      ['snap-fj', 'snap-jx'], CREATED_AT)
        first = import_package(self.target, self.dst_lake, self.packages, 'rel-idem')
        second = import_package(self.target, self.dst_lake, self.packages, 'rel-idem')
        self.assertEqual(first['objects_added'], 1)  # 两个快照共享一个对象
        self.assertEqual(second['objects_added'], 0)
        self.assertEqual(second['objects_reused'], 1)
        self.assertEqual(
            self.target.execute('SELECT count(*) FROM snapshots').fetchone()[0], 2)
        self.assertEqual(
            self.target.execute('SELECT count(*) FROM objects').fetchone()[0], 1)
        self.assertEqual(
            len(list((self.dst_lake / 'objects').rglob('*.parquet'))), 1)

    def test_identical_content_is_stored_once_even_with_other_file_names(self):
        # 源端平面文件名与对象路径完全不同，迁移后仍按内容只存一份
        flat = self.root / 'totally-different-name.parquet'
        shutil.copy(object_path(self.src_lake, self.fj_hash), flat)
        self.assertEqual(hashlib.sha256(flat.read_bytes()).hexdigest(), self.fj_hash)
        build_package(self.source, self.src_lake, self.packages, 'rel-dedup',
                      ['snap-fj', 'snap-jx'], CREATED_AT)
        import_package(self.target, self.dst_lake, self.packages, 'rel-dedup')
        stored = list((self.dst_lake / 'objects').rglob('*.parquet'))
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].name, self.fj_hash + '.parquet')

    def test_package_never_carries_source_absolute_paths(self):
        build_package(self.source, self.src_lake, self.packages, 'rel-paths',
                      ['snap-fj'], CREATED_AT)
        package_root = self.package_path('rel-paths')
        for path in package_root.rglob('*'):
            if path.is_file():
                content = path.read_bytes()
                self.assertNotIn(str(self.src_lake).encode(), content)
                self.assertNotIn(b'/workspace', content)
        manifest = json.loads((package_root / MANIFEST_NAME).read_text())
        for entry in manifest['included_objects']:
            self.assertTrue(entry['path'].startswith('objects/'))
            self.assertFalse(entry['path'].startswith('/'))

    def test_reopened_destination_still_serves_imported_snapshots(self):
        build_package(self.source, self.src_lake, self.packages, 'rel-persist',
                      ['snap-fj'], CREATED_AT)
        import_package(self.target, self.dst_lake, self.packages, 'rel-persist')
        expected = read_snapshot(self.target, self.dst_lake, 'snap-fj')
        self.target.close()
        self.target = open_store(self.dst_db)
        self.assertEqual(read_snapshot(self.target, self.dst_lake, 'snap-fj'), expected)

    def test_cli_package_and_import_roundtrip(self):
        inventory_file = self.root / 'cli-inventory.json'
        package_dir = self.root / 'cli-packages'
        commands = [
            [sys.executable, '-m', 'warehouse.cli', '--database', str(self.dst_db),
             '--lake-root', str(self.dst_lake), 'inventory', str(inventory_file)],
            [sys.executable, '-m', 'warehouse.cli', '--database', str(self.src_db),
             '--lake-root', str(self.src_lake), 'package', str(package_dir), 'cli-rel',
             'snap-fj', '--created-at', CREATED_AT],
            [sys.executable, '-m', 'warehouse.cli', '--database', str(self.dst_db),
             '--lake-root', str(self.dst_lake), 'describe-package', str(package_dir),
             'cli-rel'],
            [sys.executable, '-m', 'warehouse.cli', '--database', str(self.dst_db),
             '--lake-root', str(self.dst_lake), 'import-package', str(package_dir),
             'cli-rel'],
        ]
        # 子进程需要独立打开同一 DuckDB 文件，先释放本进程的连接锁
        self.source.close()
        self.target.close()
        try:
            for command in commands:
                completed = subprocess.run(command, capture_output=True, text=True,
                                           cwd='/workspace')
                self.assertEqual(completed.returncode, 0, completed.stderr)
            description = json.loads(subprocess.run(
                commands[2], capture_output=True, text=True).stdout)
        finally:
            self.source = open_store(self.src_db)
            self.target = open_store(self.dst_db)
        self.assertEqual(description['snapshot_ids'], ['snap-fj'])
        self.assertTrue(description['verified'])
        # 清单文件已被该命令写出，内容为合法 inventory 结构
        self.assertEqual(read_inventory(inventory_file), set())
        self.assertEqual(
            len(read_snapshot(self.target, self.dst_lake, 'snap-fj')), 2)

    def _rewrite_manifest(self, revision, manifest):
        package_root = self.package_path(revision)
        raw = canonical(manifest).encode('utf-8')
        (package_root / MANIFEST_NAME).write_bytes(raw)
        (package_root / DIGEST_NAME).write_text(
            hashlib.sha256(raw).hexdigest() + '  ' + MANIFEST_NAME + '\n',
            encoding='utf-8')

    def _assert_target_pristine(self):
        self.assertEqual(
            self.target.execute('SELECT count(*) FROM lake_tables').fetchone()[0], 0)
        self.assertEqual(
            self.target.execute('SELECT count(*) FROM snapshots').fetchone()[0], 0)
        self.assertEqual(
            self.target.execute('SELECT count(*) FROM objects').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
