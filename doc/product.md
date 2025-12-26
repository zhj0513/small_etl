# 产品引导文档

## 项目目标
- 搭建一个 ETL 系统，实现从 S3 CSV 文件到 PostgreSQL 数据库的同步入库
- 目标数据表：
  - 资产表 (asset表)
  - 交易表 (trade表)
- 实现数据验证、数据提取（包含提取字段、字段类型转换等入库前必要的数据处理）、入库、统计分析全流程
- 提供可重复执行、可测试、可扩展的数据管道

## 目标用户
- 主要用户：量化研究员 / 数据工程师
- 使用方式：Python 脚本，支持命令行参数输入
- 使用场景：测试和优化 ETL 管道

## 数据流概述

```mermaid
flowchart LR
    S3[S3 / MinIO<br/>CSV 文件]
    Polars[Polars<br/>读取 CSV]
    Validate[ValidatorService<br/>数据验证]
    DuckDB[DuckDB]
    Extract[ExtractorService<br/>字段提取与类型转换]
    Load[LoadService<br/>数据入库]
    PG[PostgreSQL]
    Analytics[AnalyticsService<br/>统计分析]
    Log[错误日志<br/>流程终止]

    S3 --> Polars
    Polars --> Validate
    Validate -->|CSV 有效| DuckDB
    Validate -->|CSV 无效| Log
    DuckDB --> Extract
    Extract --> Load
    Load --> PG
    PG --> Analytics
```

**数据处理顺序：** Assets 必须先于 Trades 处理（外键约束：Trade.account_id → Asset.account_id）

## 核心功能

### 1. 数据验证
- 使用polars读取s3上csv文件，使用Pandera验证来源csv文件是否合法。合法，传到数据提取层。如果不合法，则不再继续后续流程
- **字段级验证**：所有字段没有空值
- **业务规则验证**：
  - Asset: `total_asset = cash + frozen_cash + market_value`
  - Trade: `traded_amount = traded_price × traded_volume`

### 2. 数据提取
- 使用duckdb读取数据验证后的Polars DataFrame数据
- 支持 Hydra 配置驱动的列映射和类型转换（`configs/extractor/default.yaml`）

### 3. 数据入库
- 使用 DuckDB PostgreSQL 插件进行插入

### 4. 数据分析
- 用 DuckDB 直接查询 PostgreSQL 中的已入库数据
- 聚合统计：账户资产均值、账户资产总量

### 5. 命令行界面
- 支持 run/clean/schedule 命令
- 使用 Hydra 配置覆盖：`db=test`、`etl.batch_size=5000` 等

### 6. 定时任务管理
- APScheduler 支持周期性 ETL 执行
- 任务持久化到 PostgreSQL
- 支持添加、移除、暂停、恢复任务

### 7. Schema 管理
- SQLModel 定义数据模型
- Alembic 管理数据库迁移

### 8. 配置管理
- Hydra 多环境配置（dev/test）
- 支持环境变量覆盖

### 9. 自动化测试
- pytest 管理单元测试和集成测试
- Podman 容器自动化（PostgreSQL、MinIO）

## CLI 命令示例

```bash
# 完整 ETL 流程
pixi run python -m small_etl run

# 使用测试环境
pixi run python -m small_etl run db=test

# 覆盖多个配置
pixi run python -m small_etl run db.host=192.168.1.100 etl.batch_size=5000

# 清空数据表
pixi run python -m small_etl clean

# 定时任务管理
pixi run python -m small_etl schedule start
pixi run python -m small_etl schedule add --job-id daily_etl --etl-command run --interval day --at "02:00"
pixi run python -m small_etl schedule list
```

## 输出结果说明

### PipelineResult 结构

```python
@dataclass
class PipelineResult:
    success: bool                           # 是否成功
    started_at: datetime                    # 开始时间
    completed_at: datetime                  # 完成时间
    results: dict[str, DataTypeResult]      # 各数据类型的处理结果
    error_message: str | None               # 错误信息

@dataclass
class DataTypeResult:
    data_type: str                          # 数据类型名称 (如 "asset", "trade")
    validation: ValidationResult | None     # 验证结果
    load: LoadResult | None                 # 加载结果
    statistics: Any                         # 统计信息
```

### ValidationResult 结构

```python
@dataclass
class ValidationResult:
    is_valid: bool              # 是否验证通过
    data: pl.DataFrame          # 有效数据（验证失败时为空）
    error_message: str | None   # 错误信息
```

### 统计信息示例

```python
AssetStatistics:
  total_records: 1000
  total_cash: 50000000.00
  total_assets: 150000000.00
  avg_total_asset: 150000.00
  by_account_type: {
    2: {"count": 600, "sum_total": 90000000.00, "avg_total": 150000.00},  # 股票账户
    3: {"count": 200, "sum_total": 30000000.00, "avg_total": 150000.00},  # 信用账户
    1: {"count": 200, "sum_total": 30000000.00, "avg_total": 150000.00},  # 期货账户
  }

TradeStatistics:
  total_records: 5000
  total_volume: 500000
  total_amount: 25000000.00
  avg_price: 50.00
  by_account_type: {...}
  by_offset_flag: {48: {...}, 49: {...}}  # 买入/卖出
  by_strategy: {"策略A": {...}, "量化1号": {...}}
```

## 明确不做
- 不处理高频交易和实时流数据
- 不开发 Web 前端或 GUI
- 不直接对接外部交易系统

## 产品原则

### 1. 准确性优先
- 宁可慢一点，也要确保数据完全正确
- 有效/无效数据分离，不丢弃任何数据

### 2. 可配置性
- 提供灵活的配置选项
- 支持不同的数据源和目标格式
- 避免硬编码

### 3. 可测试性
- 重要功能需要有单元测试
- 集成测试使用 Podman 容器自动化

### 4. 可维护性
- 代码结构清晰，模块化
- 完善的文档和注释
- 遵循 Python 最佳实践
