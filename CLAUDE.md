# Project: tsdb

C 语言实现的单机高性能时序数据库，借鉴 kdb+ 设计思想。目标：在纳秒精度、列式存储、mmap I/O、SIMD 向量化、查询语言层面做到工程级可验证的性能优势。

## 项目目标

1. **数据模型**：列式 + 纳秒时间戳 + tag(symbol table) + tick/metric 双模式
2. **存储引擎**：内存层 (RDB) + 内存映射历史层 (HDB) + WAL
3. **执行引擎**：SIMD 向量化 (AVX2 / NEON) + cache-aware 算法
4. **压缩**：Delta-of-Delta + Gorilla XOR + FOR + Dict / Bitmap
5. **查询语言**：类 q 的向量化表达式 + SQL 子集 (SELECT/WHERE/GROUP BY time_bucket)
6. **基准**：对标 TSBS，ClickHouse/QuestDB/InfluxDB/VictoriaMetrics 在特定负载

## 目录结构

```
tsdb/
├─ src/          # C 源码
│  ├─ core/      # 类型、symbol table、arena
│  ├─ storage/   # WAL、列存、分区、mmap
│  ├─ compress/  # 压缩编解码
│  ├─ exec/      # SIMD 算子、聚合
│  ├─ query/     # 词法/语法/语义/计划
│  └─ server/    # TCP/HTTP 接口
├─ include/      # 公共头
├─ tests/        # 单测
├─ bench/        # 基准 vs 其他 TSDB
├─ docs/         # 设计、任务、ChangeLog（已 .gitignore）
└─ Makefile
```

## 构建

- 默认使用系统 clang/gcc，C11 标准
- `make`：Release build；`make debug`；`make test`；`make bench`
- 依赖：libc + pthread + liburing(Linux) / kqueue(macOS) / 无第三方库

## 开发约定

- 代码风格：K&R 风格；4 空格缩进；函数名 `module_action`
- 不使用 C++ 特性；不引入大型依赖
- SIMD 通过 `immintrin.h` / `arm_neon.h` + 标量 fallback
- 内存管理：arena + zone allocator，禁 `malloc` 热路径
- 错误处理：返回 `int32_t` + `errno` 风格

## 工作流

- 任务状态：`docs/tasks/xxx.md`
- 变更日志：`docs/CHANGELOG.md`
- 设计文档：`docs/design/`
- 研究笔记：`docs/research/`
- 使用 git 保存中间进度，`main` 分支不 push
