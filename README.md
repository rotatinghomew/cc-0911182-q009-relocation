# 文化资源内容寻址列式湖

这是用于本地数据接收、事实核对和列式交换的仓库服务。数据使用 DuckDB 持久化，导出为 Parquet；不需要远程数据库或运行期网络。示例数据仅为本地合成的业务记录。

## 现有业务

本地湖从资源行生成不可变 Parquet 对象，以文件 SHA-256 内容寻址；表身份包含来源，快照固定对象与行数，相同内容复用对象。`lake.create_snapshot` 生成快照，`lake.read_snapshot` 校验后读取，测试展示完整调用。对象路径相对湖根目录，目录数据库单独保存表与快照，当前没有跨部署传输或导入迁移包。

`local_exports.export_snapshot` 可将单个已验证列式对象复制为本机平面文件并登记；这不是带目录的迁移包，也不支持在另一部署恢复表身份。

## 使用

运行时为 Python 3.11.14，`requirements.txt` 精确锁定依赖版本，`requirements.lock` 保存相同版本的 Linux CPython 3.11 wheel 哈希，容器构建时校验哈希。推荐使用下列容器方式；镜像构建时准备依赖，之后运行不联网。镜像只包含本仓库应用及依赖，不启动额外服务。

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

接收包包含 `batch_id` 和 `tables`。合法字段由各表契约与领域校验共同约束；同批号同内容重送不重复写入，同业务键异内容拒绝整个接收事务。已有原始行和接收回执保留，业务值更正须采用领域中允许的独立记录，不能覆写原行。

`warehouse.storage.open_store` 打开仓库，`warehouse.ingest.ingest` 接收业务包，`warehouse.queries` 提供现有事实查询。`warehouse.export.export_tables` 固定一次读取视图并生成版本目录，`read_export` 校验成员文件摘要后读取。`samples/delivery.json` 展示各表的当前字段和合法数据。

领域校验、事实计算与查询分别存在于同一应用的不同职责中。SQL 定义保存当前存储契约；批号回执与原始行登记属于接收事务，不是业务汇总来源。文件导出只交换现有业务表，接收回执不会充当事实行。
