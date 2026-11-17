# 下游订阅出口与独立 SQLite 落地端（SUBSCRIPTIONS）

本文档描述在现有离线因果合并库上新增的**可恢复下游订阅出口**
（`mdm_merge/subscriptions.py`）和**独立 SQLite 落地端**
（`mdm_merge/landing.py`）。两者消费的都是某个副本**已经提交的可见结果**，
不是把站点交接包（`mdm-handoff/v1`）再次当作下游事件；线格式、位置坐标和
语义都是独立的一套版本化协议。

---

## 1. 两套坐标，不可互换

| 概念 | 坐标 | 含义 |
|---|---|---|
| 因果来源坐标 | `(creator, counter[, field])` | 一个字段版本**来自哪个副本的哪次操作**，随交接包传播 |
| 订阅事件位置 | `(supplier, seq)` | 一次**本地提交**在该分区变化流上的先后次序 |

`seq` 只在一次提交**真正改变了活版本集合**时分配，按本地提交顺序单调递增、
无空洞。一个很旧的交接包今天才合并，若它改变了活版本，拿到的也是**今天**
的 `seq`；只推进因果前沿、重复包、读取、压缩都不分配 `seq`。

---

## 2. 线格式（全部带 SHA-256 摘要）

所有文档都是 JSON，摘要规则与 `PROTOCOL.md` 一致：对除 `digest` 外的全部
内容做规范化编码（排序键、无空白）后取 SHA-256。

### 2.1 页令牌 `mdm-sub-token/v1`

不透明字符串 `base64url(canonical_json).sha256hex`，字段：

```
format, protocol_version=1, purpose="load"|"event",
db_uid, sub_id, supplier,
page_index            # load 令牌
after_seq             # event 令牌
```

令牌自描述且带摘要：篡改、改库（`db_uid` 不符）、改分区、改订阅、用途混用
都会被明确拒绝。令牌可在任意新进程、任意时间重放，不依赖原 Python 连接。

### 2.2 首次装载页 `mdm-sub-load/v1`

```
format, protocol_version, digest,
db_uid, replica_id, sub_id, supplier,
cut_seq,            # 该订阅的一致切点；此 seq 及之前都属于装载
page_index, page_size, total_rows, row_count,
rows: [ { asset_id, versions: [
            {field, creator, counter, value} ... ] } ... ],
next_token          # null 表示装载页结束
```

* `value` 为字符串或 `null`（已知 null）；字段缺席即“缺字段”，行缺席即删除。
* 同一令牌重试返回**相同逻辑内容**（切点物化副本，不随后续写入变化）。

### 2.3 变化事件页 `mdm-sub-event/v1`

```
format, protocol_version, digest,
db_uid, replica_id, sub_id, supplier,
begin_seq, end_seq, next_seq, page_size, event_count,
events: [ { seq, txn_kind: "local_batch"|"merge", committed_at,
            changes: [ { asset_id,
                         before: {field: [版本...]} | null,
                         after:  {field: [版本...]} | null } ... ] } ... ],
next_token
```

语义规则：

* 一次成功事务对同一行无论改几次，只发布**事务开始前**与**提交后**两个完整
  活版本集合；事务内写完又删、最终与开始完全相同的行不出现。
* 一个事务改同一 supplier 的多行，事件只有一个，下游不可能按页确认“半事务”。
* 相同文本但 `(creator,counter)` 不同，前后集合不同，仍算变化。
* `before/after` 为 `null` 表示整行不存在；字段键缺席表示该字段缺失；
  `value=null` 是已知 null——三者严格区分。
* 页按**事务数**软限额。单个超大事务即使超过软限额也**完整交付**（一页一个
  事务，`event_count=1` 而 `changes` 很多），这是唯一允许的超限额例外。

### 2.4 校验规则

* `format`/`protocol_version` 不符、结构/类型/范围错误、摘要不匹配：整体拒绝。
* 事件页内 `seq` 必须唯一且有序；版本必须含合法 `field/creator/counter/value`。
* 每页带源库身份 `db_uid` 与分区 `supplier`；落地端绑定后拒绝任何异源/异分区。

---

## 3. 源端持久状态与升级

打开旧库时 `SubscriptionManager`（以及 `Replica` 打开时的 `CREATE TABLE IF
NOT EXISTS`）自动建立下列表，即**支持打开现有数据库并原地升级**；升级前的
历史提交不会伪造事件，已有行全部通过首次装载交付：

| 表 | 规模/作用 |
|---|---|
| `sub_subscriptions` | 每订阅一行：切点 `cut_seq`、状态 `loading/live/released`、页大小、装载行数 |
| `sub_load_rows` | 每订阅的切点物化副本，SQL 端 `INSERT...SELECT` 生成，不进 Python 内存 |
| `sub_load_acks` | 已确认装载页 |
| `sub_partition` | 每分区 `earliest_seq/latest_seq`，可检查是否缺失（事件连续无洞） |
| `sub_events` | 每分区每变化事务一行（payload 即规范事件文档） |
| `sub_progress` | 每订阅独立的 `delivered_seq`（导出水位）与 `confirmed_seq`（确认水位） |
| `meta.db_uid` | 源库稳定身份，用于游标防误用 |

变化内容、位置推进与业务写入在 `import_batch`/`merge_package` 的**同一 SQLite
事务**内提交或回滚；失败输入、`os._exit` 强退都不可能留下半个事件。

**没有任何活订阅的分区不累积事件账本**：事件落盘是**按分区**判断的——只有当
某个 supplier 上存在 loading/live 订阅时，该分区的提交才写 `sub_events`。
一个批次同时涉及 X、Y 而只有 X 被订阅时，Y 不产生任何事件或位置；之后新建的
Y 订阅靠自己的切点装载取得当前行，其变化流从自己的 seq 1 连续开始。这样未被
订阅的分区与原引擎一样保持有界存储。

---

## 4. 订阅生命周期

### 4.1 创建：单事务确定切点

`create_subscription(supplier, page_size)` 在一个 `BEGIN IMMEDIATE` 事务里同时
完成：记录 `cut_seq`、把切点活版本物化进 `sub_load_rows`、初始化进度。不存在
“先扫表再取位置”的窗口。装载进行期间别的进程增删改行，后续装载页仍属于同一
切点；切点之后的提交走事件流，接完才与源库追平。

### 4.2 分页装载与确认

* `read_load_page(token)`：keyset（`LIMIT/OFFSET` 固定序）分页，只取一页数据。
* `ack_load_page(token)`：记录该页已被消费者持久落地；**导出 ≠ 确认**。
* `complete_load(sub_id)`：所有装载页都确认后才允许进入 `live`；缺页明确报错。

### 4.3 变化流与确认

* `read_event_page(token, page_size)`：从 `after_seq` 之后按事务取页，推进该
  订阅自己的 `delivered_seq`。
* `ack_events(token)`：只接受 `cut_seq ≤ seq ≤ delivered_seq` 且不回退的确认；
  跳过未交付事务、装载阶段确认、回退位置、已释放订阅一律拒绝。
* 同一 supplier 的多个订阅各有独立进度行，快慢消费者的确认**不串线**。
* 一个本地批次涉及多个 supplier 时，按分区分别发布、分别原子消费；不要求跨
  supplier 一起提交，订阅 X 的数据中永远不会出现 Y 的行或值。

### 4.4 积压、清理与释放

* `backlog(sub_id=None)`：查看每订阅的切点、分区最早/最新 seq、导出/确认水位、
  未确认事件数、保留的装载行数与分区事件行数。
* `release_subscription(sub_id)`：操作者**显式**接受“未来需要重新装载”。立即
  删除该订阅的装载副本，其令牌之后明确报错；订阅行保留为墓碑。
* `cleanup()`：只删除所有**活订阅确认水位之下**的事件（仍在装载中的订阅保护
  从 seq 1 起的全部历史）。任何活订阅还需要的事件都不会被删。
* 历史被清理后，旧游标返回明确的 `reload required` 错误，**绝不会**偷偷从当前
  最早事件继续。
* 所有消费者追平并释放后，旧事件与装载副本都可回收，不会永久保存每个历史版本。

### 4.5 与原压缩共存

`Replica.compact()` 照旧删除 `ops/op_observes`、裁剪 `dead_blob`、`VACUUM`。
变化流独立保存在 `sub_*` 表中，压缩不会禁用出口，出口也不阻止压缩；带活订阅
执行 compact 后事件流继续可读（见 `tests/test_subscription_consumers.py`）。

---

## 5. 独立 SQLite 落地端

`mdm_merge/landing.py` 的 `LandingStore` 是**可运行的参考消费者**，使用独立
数据库文件，从不打开源副本库。表：

| 表 | 作用 |
|---|---|
| `live_versions` | 已发布活版本，主键 `(supplier,asset_id,field,creator,counter)`，保留冲突与来源身份 |
| `applied_positions` | 每分区绑定的 `db_uid`、状态、`cut_seq`、已应用 `applied_seq`、当前 load_id |
| `staging_versions` / `staging_pages` | 装载暂存区与已收页摘要（含顺序与防同位异内容） |
| `applied_event_digests` | 每个已应用 seq 的内容摘要，支撑幂等与同位冲突检测 |

保证：

* **装载未完成不对外冒充新视图**：`state=loading` 时 `read_row` 直接报错；
  重新装载时旧的已发布视图在切换前仍可查询，切换（删旧、插新、翻状态、写
  切点位置）是一个事务，外部只能看到旧视图或完整新视图。
* **每个变化事务与位置更新同事务**：整页事件在一个本地事务内应用并更新
  `applied_seq`；任何一个事件校验失败，整页在业务数据改动前拒绝。
* **重复页/重复事件幂等**：已应用 seq 且摘要相同则不写业务版本；同 seq 摘要
  不同（同位异内容）在改动前拒绝。
* **乱序、缺口、异源库、错分区、损坏**：均在发布数据被修改前拒绝。
* 不要求跨库分布式事务。正确顺序是“**先落地提交，再确认源端**”；落地进程在
  确认前退出，重发后幂等继续，不允许靠先确认源端掩盖落地丢失。
* 查询接口 `read_row/list_rows/status` 可还原缺字段（`missing`）、唯一值
  （`unique`）与未解决冲突（`conflict`，多个来源版本并存）。

---

## 6. Python 接口

```python
from mdm_merge import Replica, SubscriptionManager, LandingStore

# 源端（可直接打开旧库，自动升级）
mgr = SubscriptionManager("site_a.db")
sub = mgr.create_subscription("Acme", page_size=500)

page = mgr.read_load_page(sub["first_load_token"])
# ... 把 page 交给落地端，持久化后再确认
mgr.ack_load_page(sub["first_load_token"])
# 翻页直到 page["next_token"] is None，然后：
info = mgr.complete_load(sub["sub_id"])

event_page = mgr.read_event_page(info["first_event_token"], page_size=100)
# ... 落地端 apply_event_page 成功提交后：
mgr.ack_events(event_page["next_token"])

mgr.backlog()
mgr.release_subscription(sub["sub_id"])
mgr.cleanup()

# 独立落地库
land = LandingStore("warehouse.db")
land.begin_load(first_load_page)
land.apply_load_page(page)
land.complete_load("Acme")
result = land.apply_event_page(event_page)
land.read_row("Acme", "pump-001")
```

---

## 7. 终端操作

源端与落地端是两个启动器（建议一律 `python3 -I`）：`./mdm` 与 `./mdmland`。

```bash
# 升级旧库无需单独步骤：任意 sub-* 命令打开即自动建表
python3 -I ./mdm sub-create  site_a.db Acme --page-size 500
python3 -I ./mdm sub-load    site_a.db <token-or-page-file> --out p0.json
python3 -I ./mdmland land-begin      warehouse.db p0.json
python3 -I ./mdm     sub-ack-load    site_a.db p0.json      # 或令牌
python3 -I ./mdmland land-apply-load warehouse.db p1.json
python3 -I ./mdmland land-complete   warehouse.db Acme
python3 -I ./mdm     sub-complete    site_a.db <sub_id>

python3 -I ./mdm sub-events     site_a.db <event-token> --page-size 100 --out e0.json
python3 -I ./mdmland land-apply-events warehouse.db e0.json
python3 -I ./mdm sub-ack-events site_a.db e0.json          # 落地提交之后

python3 -I ./mdm sub-backlog site_a.db [--sub-id ID]
python3 -I ./mdm sub-release site_a.db <sub_id>
python3 -I ./mdm sub-cleanup site_a.db
python3 -I ./mdmland land-status warehouse.db
python3 -I ./mdmland land-get    warehouse.db Acme pump-001
python3 -I ./mdmland land-list   warehouse.db Acme
```

`sub-load/sub-events/sub-ack-*` 的位置参数既接受原始令牌，也接受上一页导出的
页文件（自动读取其中的 `next_token`）。所有输出文件走临时文件 + 原子改名，
调用者的输入文件不会被原地改写。

### 失效后重建的标准流程

1. 源端对旧令牌报错（已释放 / 历史已回收 / 游标异源异分区）。
2. 重新 `sub-create` 得到新切点与新的首次装载令牌。
3. 落地端对新分区（或同一分区重新装载）`land-begin` → 逐页
   `land-apply-load` → `land-complete`，旧视图在切换前保持可查。
4. 从新切点的 `first_event_token` 继续变化流。

---

## 8. 验收测试

| 文件 | 覆盖 |
|---|---|
| `tests/test_subscription_flow.py` | 手写提交序列：同行多次修改、多行同事务、同值换来源、冲突变唯一、null/缺字段、末版本删除、写后删不发布、重复包/读/压缩无事件；逐事件前后状态与落地关系数据核对 |
| `tests/test_subscription_consumers.py` | 慢消费者装载期间持续写源库的切点稳定性、快消费者清理不影响慢消费者、确认不串线、多 supplier 分区隔离、超大事务整交、compact 共存、CLI 新进程往返 |
| `tests/test_subscription_failures.py` | 缺页/乱序、跳确认/回退确认、篡改页与伪造令牌、释放后令牌失效、清理后旧游标明确拒绝、异源库/错分区、重装载替换视图 |
| `tests/test_subscription_crash.py` | 真实 `os._exit`：源端事件写入中途、落地业务+位置提交中途（多点）、落地已提交未确认源端；重开重送的完整性与幂等 |
| `tests/test_subscription_scale.py` | 运行时生成 ≥10 万行源分区，小页装载+并发更新积压，记录页大小、峰值内存、积压与清理前后磁盘；固定行数追加提交后说明剩余存储构成 |

大库与变化包均在运行时生成于临时目录，不写入仓库。
