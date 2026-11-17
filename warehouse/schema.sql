CREATE TABLE IF NOT EXISTS receipts(batch_id VARCHAR PRIMARY KEY, signature VARCHAR NOT NULL, inserted INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS source_rows(table_name VARCHAR, row_key VARCHAR, payload VARCHAR NOT NULL, batch_id VARCHAR NOT NULL, PRIMARY KEY(table_name,row_key));
        CREATE TABLE IF NOT EXISTS resources(resource_id VARCHAR PRIMARY KEY, name VARCHAR, language VARCHAR);
        CREATE TABLE IF NOT EXISTS resource_rows(row_id VARCHAR PRIMARY KEY, resource_id VARCHAR, content_id VARCHAR, title VARCHAR, year INTEGER);
        CREATE TABLE IF NOT EXISTS lake_tables(table_id VARCHAR PRIMARY KEY, name VARCHAR, source_identity VARCHAR);
        CREATE TABLE IF NOT EXISTS snapshots(snapshot_id VARCHAR PRIMARY KEY, table_id VARCHAR, object_hash VARCHAR, row_count INTEGER);
        CREATE TABLE IF NOT EXISTS objects(object_hash VARCHAR PRIMARY KEY, byte_size BIGINT);

CREATE TABLE IF NOT EXISTS snapshot_exports(export_id VARCHAR PRIMARY KEY, snapshot_id VARCHAR, filename VARCHAR, object_hash VARCHAR);