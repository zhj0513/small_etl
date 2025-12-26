# Technical Steering - ETL System for S3 CSV to Database

## 核心技术栈
- Python 3.12
- 数据处理：DuckDB, PyArrow，Polars(尽量使用polars，非必要不使用pandas)
- 数据类型检测：pydantic，pandera
- ORM：SQLModel
- 数据库：PostgreSQL
- Schema 管理：Alembic
- 配置管理：Hydra
- 包管理：pixi
- CLI：Hydra（纯配置驱动，无 argparse）
- 任务调度：APScheduler（定时任务管理，PostgreSQL 持久化）
- 静态检查：pyright, pyrefly, ruff
- 测试框架：pytest

## 数据管道

### ValidatorService - 数据验证
- **Polars 读取 S3 CSV**：使用 Polars 的 `storage_options` 读取 S3 上的 CSV 文件
- **类型转换**：在验证前将金额字段转换为 Decimal(20,2) 确保精度
- **Pandera Schema 验证**：使用 `@pa.dataframe_check` 装饰器
  - 接收 `PolarsData` 参数，返回 `pl.LazyFrame`
  - 使用 `data.lazyframe.select()` 进行列式计算
- **外键验证**：检查 Trade 的 account_id 是否存在于 Asset 表中

### ExtractorService - 类型转换
- **配置驱动转换**：使用 `configs/extractor/default.yaml` 定义列映射
- **智能类型检查**：转换前检查列类型，如果已是目标类型则跳过转换
- **transform() 方法**：将验证后的 DataFrame 按配置进行类型转换

### LoaderService - 数据写入
- 使用 DuckDB 的 PostgreSQL 插件进行批量 UPSERT
- **UPDATE + INSERT 分离策略**：
  1. 注册 DataFrame 为 DuckDB 临时表
  2. UPDATE 已存在的记录
  3. INSERT 新记录（使用 NOT EXISTS 过滤）
- 原因：避免外键约束下 ON CONFLICT 的死锁问题

### AnalyticsService - 数据分析
- 用 DuckDB 直接查询 PostgreSQL 中的已入库数据（`pg.asset`, `pg.trade`）
- 支持多维度聚合：按 account_type、strategy_name、offset_flag 分组
- 聚合统计：账户资产均值、账户资产总量

## 数据处理顺序

Assets 必须先于 Trades 处理（外键约束：Trade.account_id → Asset.account_id）

**完整数据流：**
```
S3 CSV → Polars 读取 → ValidatorService 验证 → (有效→ExtractorService, 无效→日志) → 类型转换 → LoaderService 入库 → DuckDB PostgreSQL插件 → PostgreSQL → AnalyticsService 分析
```

## 必须遵守的约束
- 资产表、交易表必须用 SQLModel 定义
- 数据库操作的所有类必须有单元测试
- 金额相关的字段禁止使用 float ，统一使用 Decimal（数据库层）
- 所有时间字段统一使用 UTC
- 所有配置集中在 pyproject.toml 与 Hydra
- **数据验证优先**：必须先验证 CSV 数据的合法性，验证不通过则流程终止

### Pandera Schema 约束
- 必须使用 `@pa.dataframe_check` 装饰器进行业务规则验证
- 验证方法必须接收 `PolarsData` 参数，返回 `pl.LazyFrame`
- **字段级验证**：所有字段没有空值
- **业务规则验证**：
  - Asset: `total_asset = cash + frozen_cash + market_value`
  - Trade: `traded_amount = traded_price × traded_volume`
- **外键验证**：Trade 的 `account_id` 必须存在于已加载的 Asset 表中

### 时间戳格式
- CSV 解析格式：`%Y-%m-%dT%H:%M:%S%.f`（ISO 8601 带微秒）
- DuckDB CSV 导入格式：`timestampformat='%Y-%m-%dT%H:%M:%S'`（DuckDB `read_csv_auto` 自动推断类型时使用）
- DuckDB 读取后时区：`datetime[μs, Asia/Shanghai]`，转换时自动检测跳过

## 配置文件结构

```
configs/
├── config.yaml           # 主配置入口
├── db/
│   ├── dev.yaml          # 开发环境: etl_db
│   └── test.yaml         # 测试环境: etl_test_db
├── s3/
│   └── dev.yaml          # MinIO 配置
├── etl/
│   └── default.yaml      # batch_size
├── extractor/
│   └── default.yaml      # CSV 列映射配置
├── pipeline/
│   └── default.yaml      # Pipeline 步骤配置
└── scheduler/
    └── default.yaml      # 定时任务配置
```

### pipeline 配置格式
```yaml
# Pipeline 处理步骤 - 按顺序执行
steps:
  - data_type: asset      # 引用 DataTypeRegistry 中注册的数据类型
    enabled: true
  - data_type: trade
    enabled: true

compute_analytics: true   # 是否计算统计分析
```

### extractor 配置格式
```yaml
assets:
  columns:
    - name: account_id        # 目标列名
      csv_name: account_id    # CSV 原始列名
      dtype: Utf8             # Polars 数据类型: Int32, Int64, Float64, Utf8, Datetime
      nullable: false
    - name: updated_at
      csv_name: updated_at
      dtype: Datetime
      format: "%Y-%m-%dT%H:%M:%S%.f"  # Datetime 格式（可选）
      nullable: false

csv_options:
  delimiter: ","
  has_header: true
  encoding: "utf-8"
  null_values: ["", "NULL", "null", "None"]
```

### scheduler 配置格式
```yaml
enabled: true
check_interval: 1  # 秒
jobs:
  - job_id: daily_full_etl
    command: run          # 仅支持 run
    interval: day         # day | hour | minute
    at: "02:00"          # 仅 day 有效
    enabled: true
```

## 测试规范

### 测试结构
```
tests/
├── conftest.py              # 共享 fixtures
├── unit/                    # 单元测试（无外部依赖）
│   ├── test_analytics.py
│   ├── test_cli.py
│   ├── test_duckdb.py
│   ├── test_models.py
│   ├── test_pipeline.py
│   └── test_validator.py
└── integration/             # 集成测试（需要 PostgreSQL/MinIO）
    └── test_full_pipeline.py
```

### 集成测试容器管理
- 使用 Podman 管理 PostgreSQL 和 MinIO 容器
- `conftest.py` 中的 session-scoped fixtures 自动启动/停止容器
- 测试数据库：`etl_test_db`

### 常用 Fixtures
```python
@pytest.fixture
def test_db_engine():
    """测试数据库引擎 - 自动创建数据库和表"""

@pytest.fixture
def sample_asset_data() -> pl.DataFrame:
    """有效资产数据样本"""

@pytest.fixture
def invalid_asset_data() -> pl.DataFrame:
    """无效资产数据样本（用于验证测试）"""
```

## 错误处理规范

### ValidationResult 结构
```python
@dataclass
class ValidationResult:
    is_valid: bool              # 是否验证通过
    data: pl.DataFrame          # 有效数据（验证失败时为空）
    error_message: str | None   # 错误信息
```

### CLI 退出码
| 退出码 | 含义 |
|--------|------|
| 0 | 成功 |
| 1 | 一般错误 |

### CLI 命令
CLI 使用纯 Hydra 配置（无 argparse），所有参数通过 Hydra 覆盖语法传递。

**支持的命令（通过 `command=` 配置）:**
- `command=run` - 运行完整 ETL 流程（默认）
- `command=clean` - 清空数据表
- `command=schedule` - 定时任务管理

**Schedule 参数（通过 `job.*` 配置）:**
- `job.action` - 操作: start/add/list/remove/pause/resume
- `job.id` - 任务 ID
- `job.command` - ETL 命令: run
- `job.interval` - 间隔: day/hour/minute
- `job.at` - 执行时间 (仅 day 有效)

**Hydra 配置覆盖示例:**
```bash
pixi run python -m small_etl db=test
pixi run python -m small_etl db.host=192.168.1.100 etl.batch_size=5000
pixi run python -m small_etl command=clean
pixi run python -m small_etl command=schedule job.action=add job.id=daily_etl job.interval=day job.at=02:00
```

## 文档规范

### Docstring 格式
使用 Google 风格

## 命名约定

### Python 文件和模块
- 文件名：全小写，使用下划线分隔（`account_generator.py`）
- 类名：大驼峰命名法（`Asset`）
- 函数名：全小写，使用下划线分隔（`generate_trades`）
- 常量：全大写，使用下划线分隔（`MAX_TRADE_AMOUNT`）

### 测试文件
- 测试文件：`test_*.py` 或 `*_test.py`
- 测试类：`Test<ClassName>`
- 测试函数：`test_<功能描述>`

### 配置文件
- YAML 文件：小写，使用下划线分隔（`scenario_full.yaml`）
- 配置键：小写，使用下划线分隔

## 代码组织原则

### 模块化
- 每个模块职责单一
- 高内聚、低耦合
- 通过接口（抽象基类）定义模块边界

### 可复用性
- 公共功能提取到 `utils/`
- 使用组合而非继承（优先考虑）
- 避免循环依赖

### 可测试性
- 依赖注入而非硬编码
- 避免全局状态
- 使用 fixtures 提供测试数据

### 类型安全
- 所有函数使用类型提示
- Pydantic 模型确保数据验证
- Pyright 静态检查
