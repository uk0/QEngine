<div align="center">
  <img src="assets/logo.png" alt="QEngine logo" width="420"/>
</div>

# QEngine <sub>(Q)</sub>

C11 编写的生产级时序数据库。列式存储、SIMD 向量化执行、原始块级集群复制、专为时序+IoT 场景设计的查询语言 QTL。

> 许可证：AGPLv3 · 状态：public preview · 欢迎贡献。
>
> English: [README.md](./README.md) · 中文：本文档 · [LICENSE](./LICENSE)（法律文本以英文版为准）

---

## 30 秒跑起来

```bash
# 1. 编译（需要 clang/gcc + make + openssl）
make

# 2. 启动服务（默认 28090 客户端 / 28094 健康页）
TSDB_METRICS_BIND=127.0.0.1:28094 \
    ./build/tsdb-server --bind 127.0.0.1:28090 --data-dir /tmp/tsdb

# 3. 新开终端，跑 CLI
./build/tsdb-cli --host 127.0.0.1 --port 28090
tsdb> CREATE TABLE trades (ts TIMESTAMP, price FLOAT64, qty INT64) TIMESTAMP(ts);
tsdb> SELECT count(*) FROM trades;

# 4. 浏览器打开健康页
open http://127.0.0.1:28094/      # macOS
# 或者 xdg-open http://... (Linux)
```

**一键自检**：

```bash
bash tests/e2e_full.sh
# 构建 → 启动 → ingest 500K 行 → backup → restore → 校验 → 退出
# 最后输出 "=== Results: 24 passed, 0 failed ==="
```

---

## 能用什么

| 能力 | 说明 |
|---|---|
| **写入** | 列式 WAL + memtable（skip-list 按 ts 有序），auto-flush 至 LSM 分区 |
| **查询** | QTL（类 SQL）+ `SAMPLE BY` / `LATEST ON` / `ASOF JOIN` / `SESSION` / 窗口函数 |
| **聚合** | `count / sum / avg / min / max / p50 / p90 / p99 / stddev / first / last / twa` |
| **压缩** | 自适应 DoD / Gorilla / Chimp128 / PFOR / LZ（每块独立选） |
| **SIMD** | 运行时检测 NEON / AVX2 / AVX-512，过滤 / 聚合走向量化 |
| **UDF** | `dlopen` 动态加载 `.so`，向量化 ABI，支持 `CREATE FUNCTION ... FROM '...' SYMBOL '...'` |
| **Pub/Sub** | 实时订阅表写入（TMQ），消费组 + offset 持久化 |
| **集群** | 3 节点 + `W=N` quorum + 原始块复制 + Merkle 增量同步 |
| **联邦** | 跨集群 fanout 查询 + partial fallback |
| **安全** | PBKDF2 用户表 + RBAC（GRANT/REVOKE）+ AGPLv3 wire auth（`require_auth`）+ TLS |
| **运维** | `/health` JSON · `/metrics` Prometheus · `/` HTML dashboard |

---

## 实测数据（Apple Silicon, Release build）

```
ingest 5M 行     2.43 s   (2.05 M rows/sec)
count(*)         7 ms
avg(price)       17 ms    (5M 行)
min+max          15 ms
SAMPLE BY 1m     109 ms   (84 个桶)
WHERE + avg      16 ms
UDF × 100K 行    14 ms    (~140 ns/row 包含 TCP 往返 + CRC)
CRC32C           8153 MiB/s  (ARMv8 硬件指令)
```

完整数据见 [`INTEGRATION-REPORT.md`](./INTEGRATION-REPORT.md)。

---

## SDK — 三种语言接入

### Golang

```go
import (
    "time"
    "github.com/qengine/tsdb-go"
)

c, _ := tsdb.Open("127.0.0.1:28090", 2*time.Second)
defer c.Close()

c.CreateTable("trades", "ts", []tsdb.Column{
    {"ts",    tsdb.TypeTimestamp},
    {"price", tsdb.TypeFloat64},
})

c.WriteBatch("trades",
    []tsdb.Column{{"ts", tsdb.TypeTimestamp}, {"price", tsdb.TypeFloat64}},
    []tsdb.Row{{
        TS:  time.Now().UnixNano(),
        F64: map[int]float64{1: 99.5},
    }})

r, _ := c.Query("SELECT count(*) FROM trades")
// r.Rows[0][0] is int64
```

源码 + README：[`sdk/go/`](./sdk/go/)

### Java + JDBC

```java
Class.forName("com.tsdb.jdbc.TsdbDriver");
try (Connection c = DriverManager.getConnection(
         "jdbc:tsdb://127.0.0.1:28090");
     Statement st = c.createStatement();
     ResultSet r  = st.executeQuery("SELECT count(*) FROM trades")) {
    while (r.next()) System.out.println(r.getLong(1));
}
```

编译：`cd sdk/java && mvn package`（或直接 `javac --release 11`）。

源码 + README：[`sdk/java/`](./sdk/java/)

### QTL（服务端脚本）

```sql
-- 写入的表就能直接查
SELECT avg(price), max(qty)
FROM trades
WHERE ts >= '2026-04-18T00:00:00Z'
SAMPLE BY 1m
LIMIT 60;

-- 注册 UDF
CREATE FUNCTION my_double(FLOAT64) RETURNS FLOAT64
    FROM '/opt/qengine/udf/my_lib.so' SYMBOL 'udf_double';

SELECT my_double(price) FROM trades LIMIT 1000;

-- RBAC
CREATE USER ops IDENTIFIED BY 'xxxx' ROLE normal;
GRANT SELECT ON trades TO ops;
```

完整语法：[`README.md` 的 QTL 段落](./README.md)。

---

## 四个运维 CLI

全部基于 Go SDK，`go build` 即得：

```bash
cd sdk/go && go build -o bin/tsdb-backup  ./cmd/tsdb-backup
            go build -o bin/tsdb-restore ./cmd/tsdb-restore
            go build -o bin/tsdb-import  ./cmd/tsdb-import
            go build -o bin/tsdb-export  ./cmd/tsdb-export
```

| 工具 | 用法 |
|---|---|
| `tsdb-backup`  | 导出指定表 → `manifest.json + *.csv` 目录 |
| `tsdb-restore` | 读 backup 目录 → 重建 schema + 回灌 |
| `tsdb-import`  | 读 CSV → `WRITE_BATCH` 批量写入（schema 自动识别） |
| `tsdb-export`  | 跑任意 QTL → CSV（`-ts-rfc3339` 切到人类可读时间） |

**灾备三连示例**：

```bash
# 备份
sdk/go/bin/tsdb-backup  -addr 127.0.0.1:28090 -out /backup/20260418 -tables trades,metrics

# 灾难发生后在新机器恢复
sdk/go/bin/tsdb-restore -addr 127.0.0.1:28090 -in  /backup/20260418

# 临时取数
sdk/go/bin/tsdb-export  -addr 127.0.0.1:28090 \
    -qtl "SELECT count(*), avg(price) FROM trades" \
    -out report.csv
```

---

## 健康监控

服务启动后，浏览器打开 `http://host:28094/` 即可看到实时 dashboard（每 2 秒刷新）：

- 运行时长 / 活跃连接数
- 累计查询 / 写入 / 错误
- Bloom 过滤命中率
- 认证登录 / 拒绝次数

Kubernetes / 负载均衡探针走 `/health`：

```yaml
readinessProbe:
  httpGet: { path: /health, port: 28094 }
  periodSeconds: 5
```

Prometheus 抓取：

```yaml
scrape_configs:
  - job_name: tsdb
    static_configs:
      - targets: ['tsdb-host:28094']
```

---

## 多节点集群（3 节点示例）

```bash
# 三个节点各自配置 tsd.conf（data_dir / cluster_seeds / 端口不同）
./build/tsdb-server --config conf/node1.conf &
./build/tsdb-server --config conf/node2.conf &
./build/tsdb-server --config conf/node3.conf &

# 整个集群验证（会启动 3 节点，杀掉一个，验证 W=2 写入仍成功）
make test-cluster
```

看 `deployment/` 目录有 Dockerfile + entrypoint 示例。

---

## 测试

```bash
make test            # 45 套核心测试（~45 秒，全绿）
make test-cluster    # 3 节点集群 + 节点宕机容错 + Merkle 同步
make test-federation # 跨集群联邦查询
bash tests/e2e_full.sh   # 端到端运维场景（24 断言，~30 秒）
```

---

## 目录结构

```
tsdb/
├── src/                 # C11 引擎源码
│   ├── core/            # 类型 / symbol table / arena
│   ├── storage/         # WAL / 列存 / 分区 / mmap / skip-list
│   ├── compress/        # 压缩编解码
│   ├── exec/            # SIMD 算子 / 聚合 / t-digest
│   ├── query/           # 词法 / 语法 / 计划 / 执行
│   ├── catalog/         # STable / group / device / RBAC / UDF / TMQ
│   ├── cluster/         # 集群 / 复制 / Merkle / autobalance
│   ├── federation/      # 跨集群联邦查询
│   └── server/          # TCP / HTTP / TLS / /metrics /health
├── sdk/
│   ├── go/              # Go SDK + bench + 四个 CLI 工具
│   └── java/            # Java SDK + JDBC driver
├── tests/               # 单测 + e2e_full.sh
├── bench/               # TSBS + 压缩 + I/O 基准
├── cli/                 # tsdb-cli, tsdb-client REPL
├── deployment/          # Dockerfile + docker-compose
├── include/tsdb.h       # 公共 C API
├── INTEGRATION-REPORT.md # 全量测试 + 性能报告
├── LICENSE              # AGPLv3（法律文本）
├── LICENSE.zh-CN.md     # AGPLv3 中文参考（非官方）
├── README.md            # English
└── README.zh-CN.md      # 本文件
```

---

## 许可证

**AGPLv3** — 见 [`LICENSE`](./LICENSE)（英文法律文本为准）和
[`LICENSE.zh-CN.md`](./LICENSE.zh-CN.md)（非官方中文参考）。

关键条款（AGPLv3 §13）：任何人把 QEngine 改造成**网络服务**对外提供给
远程用户时，必须把修改过的源码以 AGPLv3 许可证交给这些用户。纯自用 /
纯内部部署不触发此条款。

---

## 贡献

- Issue / PR 至 GitHub 仓库
- 测试先行：任何新功能都需伴随 `tests/*.c` 的覆盖
- Commit 信息：`<area>(<scope>): <imperative>`（参考 `git log --oneline`）
- 风格：C 部分 K&R + 4 空格；Go 部分 `gofmt -s`；Java 部分 IntelliJ 默认格式
