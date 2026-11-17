
import copy
import json
import tempfile
import unittest
from pathlib import Path

from warehouse.export import export_tables, read_export
from warehouse.ingest import ingest
from warehouse.queries import overview
from warehouse.storage import open_store


class WarehouseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.connection = open_store(self.root / 'store.duckdb')
        self.payload = json.loads(Path('samples/delivery.json').read_text())

    def tearDown(self):
        self.connection.close()
        self.temp.cleanup()

    def load(self):
        return ingest(self.connection, copy.deepcopy(self.payload))

    def test_replay_keeps_source_rows(self):
        first = self.load()
        second = self.load()
        self.assertGreater(first['inserted'], 0)
        self.assertEqual(second['inserted'], 0)
        self.assertTrue(second['replayed'])
        self.assertEqual(self.connection.execute('SELECT count(*) FROM receipts').fetchone()[0], 1)

    def test_batch_collision_is_atomic(self):
        self.load()
        collision = copy.deepcopy(self.payload)
        collision['tables'] = {}
        with self.assertRaises(ValueError):
            ingest(self.connection, collision)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM receipts').fetchone()[0], 1)

    def test_unknown_table_does_not_write_receipt(self):
        invalid = {'batch_id': 'bad', 'tables': {'missing_table': []}}
        with self.assertRaises(ValueError):
            ingest(self.connection, invalid)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM receipts').fetchone()[0], 0)

    def test_different_batch_same_rows_is_not_duplicate(self):
        first = self.load()
        duplicate = copy.deepcopy(self.payload)
        duplicate['batch_id'] = 'retry-another-envelope'
        self.assertEqual(ingest(self.connection, duplicate)['inserted'], 0)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM source_rows').fetchone()[0], first['inserted'])

    def test_export_roundtrip_and_hash(self):
        self.load()
        result = export_tables(self.connection, self.root / 'exports', 'release1')
        for table, entry in result['tables'].items():
            actual = read_export(self.connection, self.root / 'exports', 'release1', table)
            self.assertEqual(len(actual), entry['rows'])
            self.assertEqual(set(actual[0]) if actual else set(),
                             set(self.payload['tables'][table][0]) if actual else set())
        table = next(iter(result['tables']))
        output = self.root / 'exports' / 'release1' / (table + '.parquet')
        output.write_bytes(output.read_bytes() + b'broken')
        with self.assertRaises(ValueError):
            read_export(self.connection, self.root / 'exports', 'release1', table)

    def test_export_names_and_immutability(self):
        self.load()
        with self.assertRaises(ValueError):
            export_tables(self.connection, self.root, '../escape')
        export_tables(self.connection, self.root, 'stable')
        with self.assertRaises(FileExistsError):
            export_tables(self.connection, self.root, 'stable')

    def test_reopen_preserves_data(self):
        self.load()
        expected = overview(self.connection)
        self.connection.close()
        self.connection = open_store(self.root / 'store.duckdb')
        self.assertEqual(overview(self.connection), expected)

    def test_rejected_record_has_no_partial_state(self):
        invalid = copy.deepcopy(self.payload)
        populated = next(rows for rows in invalid['tables'].values() if rows)
        populated.append({})
        with self.assertRaises(ValueError):
            ingest(self.connection, invalid)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM source_rows').fetchone()[0], 0)

    def test_snapshot_roundtrip_and_reused_objects(self):
        from warehouse.lake import create_snapshot,read_snapshot
        self.load()
        first=create_snapshot(self.connection,self.root/'lake','fujian','t1','s1','office-a')
        second=create_snapshot(self.connection,self.root/'lake','fujian','t1','s2','office-a')
        self.assertEqual(first['object_hash'],second['object_hash'])
        self.assertEqual(len(read_snapshot(self.connection,self.root/'lake','s1')),2)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM objects').fetchone()[0],1)

    def test_corrupted_object_is_not_read(self):
        from warehouse.lake import create_snapshot,read_snapshot,object_path
        self.load()
        result=create_snapshot(self.connection,self.root/'lake','fujian','t1','s1','office-a')
        object_path(self.root/'lake',result['object_hash']).write_bytes(b'corrupt')
        with self.assertRaises(ValueError):
            read_snapshot(self.connection,self.root/'lake','s1')

    def test_table_source_identity_is_bound(self):
        from warehouse.lake import create_snapshot
        self.load()
        create_snapshot(self.connection,self.root/'lake','fujian','t1','s1','office-a')
        with self.assertRaises(ValueError):
            create_snapshot(self.connection,self.root/'lake','fujian','t1','s2','office-b')


    def test_local_export_tracks_verified_object(self):
        from warehouse.lake import create_snapshot
        from warehouse.local_exports import export_snapshot
        self.load()
        create_snapshot(self.connection,self.root/'lake','fujian','t1','s1','office-a')
        filename=export_snapshot(self.connection,self.root/'lake','s1',self.root/'local','export1')
        self.assertTrue(Path(filename).is_file())
        self.assertEqual(export_snapshot(self.connection,self.root/'lake','s1',self.root/'local','export1'),filename)
