# 文化资源内容寻址列式湖

这是用于本地数据接收、事实核对和列式交换的仓库服务。数据使用 DuckDB 持久化，导出为 Parquet；不需要远程数据库或运行期网络。示例数据仅为本地合成的业务记录。

## 现有业务

本地湖从资源行生成不可变 Parquet 对象，以文件 SHA-256 内容寻址；表身份包含来源，快照固定对象与行数，相同内容复用对象。`lake.create_snapshot` 生成快照，`lake.read_snapshot` 校验后读取，测试展示完整调用。对象路径相对湖根目录，目录数据库单独保存表与快照，当前没有跨部署传输或导入迁移包。

`local_exports.export_snapshot` 可将单个已验证列式对象复制为本机平面文件并登记；这不是带目录的迁移包，也不支持在另一部署恢复表身份。

## 跨部署快照迁移

`warehouse.relocation` 在不开放原库、不整目录复制的前提下，把明确选择的表快照打包为自描述迁移包。包目录 `<destination>/<revision>/` 只包含：

- `manifest.json`：格式版本、表身份（`table_id`/`name`/`source_identity`）、选定快照（对象摘要与行数）、随包对象（内容寻址相对路径与字节大小）、`required_objects` 先前内容依赖；
- `manifest.sha256`：清单本身的 SHA-256，防止传输中篡改声明；
- `objects/<xx>/<sha256>.parquet`：仅包含选定快照引用且接收端尚缺的对象，全部为相对内容寻址路径，不含源机器绝对路径或未选择的表。

典型离线流程：

```bash
# 接收端导出已有对象清单（可通过离线介质回传）
python -m warehouse.cli --database receiver.duckdb --lake-root receiver-lake \
    inventory receiver-inventory.json
# 发送端按快照标识选择打包，带清单时只补传缺少的对象
python -m warehouse.cli --database source.duckdb --lake-root source-lake \
    package ./relocation rel-2026-11 snap-fj snap-zj \
    --inventory receiver-inventory.json
# 接收端先核对迁移范围，再校验导入
python -m warehouse.cli --database receiver.duckdb --lake-root receiver-lake \
    describe-package ./relocation rel-2026-11
python -m warehouse.cli --database receiver.duckdb --lake-root receiver-lake \
    import-package ./relocation rel-2026-11
```

安全与一致性保证：

- **增量与依赖**：接收端清单中已有的对象不重复入包，只在 `required_objects` 中声明；导入时依赖必须同时具备目录登记和完好字节内容，对象文件存在但缺少目录成员不算满足依赖。
- **内容寻址去重**：即使源端平面文件名或路径不同，相同 SHA-256 的对象在接收端只存一份，多个快照可共享。
- **身份保护**：`table_id` 同标识但名称/来源不同、或接收端已有同名不同来源的表都会拒绝导入，绝不按名字覆盖。
- **传输校验**：清单摘要、随包对象逐字节 SHA-256、字节大小、文件清单（拒绝多余/缺失文件与符号链接）、快照行数全部校验；缺对象、损坏对象或依赖不满足时不会写入任何目录成员。
- **失败可重试**：对象先写入临时文件再原子改名，目录成员与 `lake.read_snapshot` 正式接口回读在同一事务内完成，校验失败随事务回滚并清理本次新对象，接收端原有可用目录保持不变，修复后可原样重试，重复导入幂等。
- **范围可核对**：`describe_package` 与导入结果返回表、快照、随包对象和依赖对象清单；未选择的表与源端绝对路径不会出现在包内（有专门测试扫描全部包文件验证）。


## 使用

运行时支持 Python 3.11.x，独立容器使用摘要固定的 Python 3.11 镜像。`requirements.txt` 精确锁定依赖版本，`requirements.lock` 保存相同版本的 Linux CPython 3.11 wheel 哈希，容器构建时校验哈希。推荐使用下列容器方式；镜像构建时准备依赖，之后运行不联网。镜像只包含本仓库应用及依赖，不启动额外服务。

```bash
docker build -t relocation-warehouse .
docker run --rm --network none relocation-warehouse
```

已经准备依赖的环境中也可以直接执行：

```bash
python -m unittest discover -s tests -v
python -m compileall -q warehouse tests
python -m warehouse.cli --database /tmp/relocation.duckdb load samples/delivery.json
python -m warehouse.cli --database /tmp/relocation.duckdb overview
python -m warehouse.cli --database /tmp/relocation.duckdb export /tmp/relocation-exports release1
```

上面的导出版本不可覆盖；再次核对应使用新的数据库路径和新的导出目录，或直接运行使用临时目录的测试。测试同时验证接收事务、业务主键冲突、批号重送、持久化及列式文件回读。完整测试和样本运行通常数秒结束，没有等待外部服务的步骤。

## 数据与接口

构建所需工具和传递依赖同样精确锁定，并包含在容器依赖中。执行 `python -m build --wheel --no-isolation` 可在不下载额外构建依赖的情况下生成 wheel；SQL 表定义随包分发。

接收包包含 `batch_id` 和 `tables`。合法字段由各表契约与领域校验共同约束；同批号同内容重送不重复写入，同业务键异内容拒绝整个接收事务。已有原始行和接收回执保留，业务值更正须采用领域中允许的独立记录，不能覆写原行。

`warehouse.storage.open_store` 打开仓库，`warehouse.ingest.ingest` 接收业务包，`warehouse.queries` 提供现有事实查询。`warehouse.export.export_tables` 固定一次读取视图并生成版本目录，`read_export` 校验成员文件摘要后读取。`warehouse.relocation` 提供跨部署的快照迁移包：`write_inventory`/`read_inventory` 交换对象清单，`build_package` 按选定快照打包，`describe_package` 核对范围，`import_package` 校验并原子导入。`samples/delivery.json` 展示各表的当前字段和合法数据。

领域校验、事实计算与查询分别存在于同一应用的不同职责中。SQL 定义保存当前存储契约；批号回执与原始行登记属于接收事务，不是业务汇总来源。文件导出只交换现有业务表，接收回执不会充当事实行。
