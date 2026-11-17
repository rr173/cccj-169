# mdm_merge — 离线多副本主数据因果合并库

多个采集站各自长期维护同一份设备主数据，离线时只能交换数据包，没有中心节点、
不按墙上时钟或到达顺序裁定先后。本库用纯 Python + SQLite 实现面向主数据管道的
多副本合并：本地整理批次、离线交接包、带未解决冲突的关系数据输出。

## 1. 数据模型与不变量

* 每个副本有独立且持久的 `replica_id`（创建时固定，存于 `meta` 表）。
* 同步分区按 `supplier` 划分；行键为 `(supplier, asset_id)`。
* 业务字段固定为 `location`、`model`、`category`，值只能是字符串或 `null`。
* 字段版本的身份是来源坐标 `(creator, counter[, field])`，**不是值**。
  同字段并发值即使文本完全相同也保留各自来源，绝不按值去重。
* `null` 是一个已知值，与“字段缺失”不同；`put` 未点名的字段保持不变。
* 行是否存在取决于是否还有活的字段版本；其余字段可以缺失。

### 观察（observation）与因果

“观察到”只取**已提交的导入/合并**产生的因果关系，不看时钟、包到达次序或
`replica_id` 排序。

* 每次本地 `put`/`erase` 取得一个不可复用的来源坐标 `(本副本, 自增 counter)`。
* `put` 观察（压掉）该字段当前所有活版本；`erase` 观察该行当前所有活字段版本。
* 同一批次后面的操作能看到前面操作的结果。
* 一个字段版本存活，当且仅当不存在任何已整合的、观察到它的操作。
* 不同字段的并发修改各自独立生效；同字段的并发版本以多版本冲突同时存活。

按 `(行, 字段, creator)` 维护**前缀死亡水位线** `dead_counter`：该 creator 这条
因果链上所有 `counter <= dead_counter` 的版本都已被观察（死亡）。因此完整操作
历史可压缩为每键一个整数水位，无需永久保存事件账本。

## 2. 本地操作语义

* `put`：对点名的字段，用新坐标写入值，并把该字段当前活版本记入观察集（压掉
  自己已观察到的竞争版本）；没见过的并发版本不受影响。未点名字段不变。
* `erase`：删除该副本当前已观察到的这行全部字段版本（逐字段记录观察）。
  `erase` 与未见过的 `put` 并发时，那个 `put` 存活；待收到并发值后再次 `erase`
  才能删掉它。
* 用户解决冲突 = 先合并目前资料，再做一次普通 `put`：它压掉**已看到**的竞争
  版本，但覆盖不了尚未收到的新竞争者。

## 3. 交接包格式（见 `mdm_merge/protocol.py`）

两类 JSON 包，均带 `protocol_version`、显式 `kind`、作用域 `supplier` 与
规范化 SHA-256 `digest`（对除 `digest` 外的全部内容做确定性编码后取摘要）。

### 检查点 `checkpoint`

某个 `supplier` 分区的完整自洽快照：

* `adds`：当前所有活字段版本；
* `kills`：每个 `(行,字段,creator)` 的前缀死亡水位线；
* 为每条 `kill` 配套其被删坐标的 **add 事实**（水位尖端值 + 行级因果摘要
  `dead_blob` 里保留的死坐标），使没见过该 add 的副本也能应用 remove，而不会
  因乱序把值复活或永久卡住；
* `frontier`：该分区各副本的完整因果前沿摘要。

### 差量 `delta`

针对一份**本副本已导出的检查点**之后的变化：

* 只含基线之后的新增活版本、新墓碑与抬升的水位，绝不含未改动业务行；
* 携带 `baseline.checkpoint_id` 与基线 `frontier`，范围与覆盖语义显式；
* 同样为新 remove 配套必要的 add 事实。

接收方**不必先拥有基线**：可先收差量、后收旧检查点。针对尚不知道的 add 的
remove 进入 `pending_kills` 缓冲，等该 add（精确坐标，或检查点/差量带来的配套
add）到达后再释放，因此乱序、重复、经第三站转发都收敛到同一结果。

> 差量只能由导出该基线检查点的副本生成（它留存了基线快照用于逐键 diff）。
> 压缩后若基线快照已不在，请求方应改用完整检查点——这是显式报错，不是静默
> 发整分区伪装差量。

## 4. 合并规则（OR-set + 前缀水位）

合并是幂等且与顺序无关的并集操作：

1. 先整合 `frontier`（独立的并集摘要）。
2. 整合 `adds`：每键只保留最高 counter 的当前行；是否存活由水位在提交时统一
   重算，不在行上缓存。
3. 所有远端 `kills` 先进 `pending_kills`；随后逐**精确坐标**释放——只有该坐标
   的 add 事实已在本地（当前行、本包 incoming add、或水位行的 `dead_blob`/
   `dead_value`）才应用。不同 counter 互不抬举，高墓碑不能借低版本的 add 生效。
4. 提交前对整个 supplier 重算所有 `live` 标志，保证包内 add/kill 次序无关。

只同步 supplier X 的包不会触碰 Y；包中没列出的行绝不会被删除。

## 5. 持久状态、压缩与规模

稳态表（均与操作次数无关，只取决于接触过的键、参与副本、未闭合因果缺口）：

| 表 | 条目规模 | 作用 |
|---|---|---|
| `field_state` | 每 `(行,字段,creator)` 一行 | 当前竞争版本（最高 counter）+ 存活标志 |
| `dead_watermark` | 每 `(行,字段,creator)` 一行 | 前缀水位 + 尖端值 + 有界死坐标摘要 `dead_blob` |
| `pending_kills` | 仅未闭合的乱序缺口 | 等 add 到达即 drain 到零 |
| `replica_frontier` | 副本数 × supplier | 因果前沿 |
| `checkpoints` | 压缩后每 supplier 仅留最新一个基线 | 差量逐键 diff 依据 |

`compact()` 删除 `ops`/`op_observes` 操作账本，把每个 `dead_blob` 裁剪到最近
`_blob_keep`（默认 64）条，并把 `checkpoints` 基线剪到每 supplier 最新一个，再
`VACUUM`。压缩后重收很旧的检查点仍不能复活旧值：旧 add 的 counter 不超过水位线，
落地即死（born-dead）。

规模验证（`tests/test_scale_bounded.py`）分三阶段：N、2N、3N 次已收敛覆盖/删除。
第一阶段 blobs 仍在向每键上限填充，磁盘可能小幅上升；进入第二、三阶段后所有表都
受“接触过的键”封顶，逻辑条目与 SQLite 回收占用都进入平台期（实测逻辑条目比 ≈1.00、
磁盘比 ≈1.04），证明没有保存随操作次数增长的事件账本。

哪些量可能因乱序暂时增长、如何收敛：

* `pending_kills` 随“remove 已到、对应 add 未到”的因果缺口增长；缺口随配套 add
  到达而 drain，全量检查点交换后归零。
* `dead_blob` 随被删不同坐标增长，但压缩按键封顶；前缀水位本身始终只有一行。
* 绝不以“丢弃旧包的时间期限”代替正确合并。

## 6. 身份与备份恢复约束

* 信任合法副本生成的来源坐标与因果声明，不做拜占庭容错。
* `replica_id` 不得克隆后双写：两份库若用同一 `replica_id` 各自产生新坐标，会
  制造相同 `(creator, counter)` 但语义不同的版本，破坏坐标唯一性。
* 备份恢复只能在**原副本停机、无分叉写入**时用备份镜像顶替原副本（同一身份、
  同一 counter 序列的延续）。需要让一个备份独立存活时，必须先分配新的
  `replica_id`，并把它当作一个新参与者，绝不复用旧身份继续发号。
* 本地批次与包合并都在单个 SQLite 事务内整体可见；进程中途强退，未提交事务整体
  回滚，业务值与因果摘要不可能只更新一半，重送完整包即可继续。

## 7. 输入校验

* 明确的协议版本、结构、类型、范围检查；未知字段、未知 op、非法标量、跨 supplier
  记录、截断/损坏 JSON、摘要不匹配一律整体拒绝，不部分混入。
* 差量校验记录去重与形状；“是否相对基线真实变化”由导出端逐键快照 diff 保证。
* 用户文件与交接包从不原地改写：输出走临时文件 + 原子 `os.replace`。

## 8. Python 接口

```python
from mdm_merge import Replica

rep = Replica.create("a.db", replica_id="A")
rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "r",
                   "fields": {"location": "L0"}}])
rep.export_checkpoint("S", "cp.json")
rep.merge_file("peer.json")
rep.export_delta("S", baseline_dict, "delta.json")
rep.read_row("S", "r")          # 逐字段活版本 + missing/unique/conflict
rep.read_partition("S")
rep.stats()                     # 活版本、摘要条目、实际占用
rep.compact()
```

## 9. 终端入口

```
./mdm init DB [--replica-id ID]
./mdm batch DB batch.json
./mdm checkpoint DB supplier out.json
./mdm delta DB supplier baseline.json out.json
./mdm merge DB package.json
./mdm get DB supplier asset_id
./mdm list DB supplier
./mdm suppliers DB
./mdm stats DB
./mdm compact DB
./mdm inspect package.json
```

所有调用建议用 `python3 -I` 运行，避免当前目录里的同名模块（如伪造的 `json.py`）
被解释器加载。

## 10. 验收测试

* `tests/demo_scenarios.py`：三个真实 SQLite 副本 + 新进程 CLI，逐步手写期望的
  活版本身份（erase/未见 put、M1/M2→M3、null/缺字段、同批先写后删、单 supplier、
  差量先于基线、第三方转发、任意重复、损坏包拒绝等）。
* `tests/oracle.py` + `tests/test_fuzz_vs_oracle.py`：独立的操作因果图参考器
  （不调用生产合并/压缩），随机本地操作与乱序/重复/转发交付，逐副本对照生产库
  最终活版本，并验证当前检查点交换后三副本收敛。
* `tests/test_protocol_validation.py`：损坏/篡改/截断/越界/未知结构输入整体拒绝、
  拒绝不产生部分写入、合法包幂等、空差量无害。
* `tests/test_crash_atomicity.py`：在合并事务各步真实 `os._exit` 强退后重开，
  校验业务值与因果摘要共同提交、可重送完整包继续。
* `tests/test_scale_bounded.py`：固定三副本、少量行，N→2N→3N 次已收敛覆盖/删除
  （默认 N=5 万），压缩重开后量逻辑条目与 SQLite 回收占用/耗时，证明进入平台期
  而非保存事件账本，最后投递最早旧包确认不复活。大测试数据运行时生成。
