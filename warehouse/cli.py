import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from .export import export_tables
from .ingest import ingest
from .queries import overview
from .relocation import (build_package, describe_package, import_package,
                         read_inventory, write_inventory)
from .storage import open_store


def main():
    parser = argparse.ArgumentParser(description='本地仓库日常接收与核对')
    parser.add_argument('--database', default='warehouse.duckdb')
    parser.add_argument('--lake-root', default='lake', help='内容寻址对象湖根目录')
    commands = parser.add_subparsers(dest='action', required=True)
    load = commands.add_parser('load', help='接收一个业务包')
    load.add_argument('file', type=Path)
    commands.add_parser('overview', help='查看现有业务汇总')
    output = commands.add_parser('export', help='冻结列式导出')
    output.add_argument('destination', type=Path)
    output.add_argument('revision')
    inventory = commands.add_parser('inventory', help='导出本部署已有对象清单')
    inventory.add_argument('file', type=Path)
    package = commands.add_parser('package', help='按选定快照构建迁移包')
    package.add_argument('destination', type=Path)
    package.add_argument('revision')
    package.add_argument('snapshots', nargs='+', help='要迁移的快照标识')
    package.add_argument('--inventory', type=Path, help='接收端对象清单，用于增量打包')
    package.add_argument('--created-at', default='', help='构建时间说明')
    inspect = commands.add_parser('describe-package', help='核对并描述迁移包范围')
    inspect.add_argument('destination', type=Path)
    inspect.add_argument('revision')
    receive = commands.add_parser('import-package', help='校验并导入迁移包')
    receive.add_argument('destination', type=Path)
    receive.add_argument('revision')
    args = parser.parse_args()
    connection = open_store(args.database)
    try:
        if args.action == 'load':
            result = ingest(connection, json.loads(args.file.read_text(encoding='utf-8')))
        elif args.action == 'export':
            result = export_tables(connection, args.destination, args.revision)
        elif args.action == 'inventory':
            result = {'objects': write_inventory(args.file, connection)}
        elif args.action == 'package':
            present = read_inventory(args.inventory) if args.inventory else None
            created_at = args.created_at or datetime.now(timezone.utc).isoformat()
            result = build_package(connection, args.lake_root, args.destination,
                                   args.revision, args.snapshots, created_at, present)
        elif args.action == 'describe-package':
            result = describe_package(args.destination, args.revision)
        elif args.action == 'import-package':
            result = import_package(connection, args.lake_root, args.destination, args.revision)
        else:
            result = overview(connection)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
