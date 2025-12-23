# 架构设计文档

## 1. 系统概览

Small ETL 是一个批量数据 ETL 系统，用于将 S3 CSV 文件中的资产和交易数据同步到 PostgreSQL 数据库，并提供数据验证和统计分析功能。

### 1.1 系统架构图

```
flowchart LR
    subgraph SYS["Small ETL System"]
        S3["S3 Source\n(CSV Files)"] --> ETL["ETL Pipeline"]
        ETL --> PG["PostgreSQL\n(Database)"]
        ETL --> DuckDB["DuckDB\n(Validator)"]
    end
```

### 1.2 技术栈

| 层级 | 技术选型 | 用途 |
|------|----------|------|
| 数据源 | AWS S3 | CSV 文件存储 |
| 数据验证 | DuckDB | 内存数据验证、字段完整性检查 |
| 目标数据库 | PostgreSQL | 持久化存储 |
| ORM/Schema | SQLModel | 数据模型定义 |
| 数据库迁移 | Alembic | Schema 版本管理 |
| 配置管理 | Hydra | 多环境配置管理 |
| 包管理 | pixi | 依赖和环境管理 |
| 测试框架 | pytest | 单元测试和集成测试 |
| 类型检查 | Pyright | 静态类型检查 |

## 2. 分层架构

### 2.1 架构层次

```
flowchart TB
    %% ======================
    %% CLI Layer
    %% ======================
    CLI["CLI Layer<br/>(命令行层)<br/>- 参数解析<br/>- 任务调度"]

    %% ======================
    %% Application Layer
    %% ======================
    APP["Application Layer<br/>(应用层)<br/>- 业务流程编排<br/>- 错误处理<br/>- 日志记录"]

    %% ======================
    %% Service Layer
    %% ======================
    subgraph SERVICE["Service Layer (服务层)"]
        EX["Extractor<br/>Service"]
        VA["Validator<br/>Service"]
        LO["Loader<br/>Service"]
        AN["Analytics<br/>Service"]
        SM["Schema<br/>Manager"]
    end

    %% ======================
    %% Data Access Layer
    %% ======================
    subgraph DAL["Data Access Layer (数据访问层)"]
        S3["S3<br/>Connector"]
        DUCK["DuckDB<br/>Client"]
        PG["PostgreSQL<br/>Repository"]
    end

    %% ======================
    %% Domain Layer
    %% ======================
    DOMAIN["Domain Layer<br/>(领域层)<br/>- 数据模型 (Asset, Trade)<br/>- 业务规则<br/>- 验证逻辑"]

    %% ======================
    %% Layer Connections
    %% ======================
    CLI --> APP
    APP --> SERVICE
    SERVICE --> DAL
    DAL --> DOMAIN
```

### 2.2 各层职责

#### CLI Layer (命令行层)
- 解析命令行参数
- 调用应用层服务
- 格式化输出结果

#### Application Layer (应用层)
- ETL 流程编排
- 全局错误处理
- 日志和监控
- 事务管理

#### Service Layer (服务层)
- **Extractor Service**: 从 S3 提取 CSV 数据
- **Validator Service**: 使用 DuckDB 验证数据
- **Loader Service**: 将数据加载到 PostgreSQL
- **Analytics Service**: 数据统计分析
- **Schema Manager**: 管理数据库 Schema 迁移

#### Data Access Layer (数据访问层)
- S3 文件读取接口
- DuckDB 操作封装
- PostgreSQL 数据库操作 (CRUD)

#### Domain Layer (领域层)
- 数据模型定义 (SQLModel)
- 业务规则和验证逻辑
- 领域事件

## 3. 核心组件设计

### 3.1 ETL Pipeline 组件

```
┌─────────────────────────────────────────────────────────┐
│                    ETL Pipeline                          │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐      │
│  │ Extract  │─────▶│ Validate │─────▶│  Load    │      │
│  └──────────┘      └──────────┘      └──────────┘      │
│       │                  │                  │            │
│       ▼                  ▼                  ▼            │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐      │
│  │ S3 CSV   │      │ DuckDB   │      │PostgreSQL│      │
│  │ Reader   │      │ Checker  │      │ Writer   │      │
│  └──────────┘      └──────────┘      └──────────┘      │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

#### 3.1.1 Extractor (提取器)

**职责:**
- 连接 S3 存储
- 读取 CSV 文件
- 解析为 Python 对象
- 批量处理大文件

**接口:**
```python
class Extractor(Protocol):
    def extract(self, source_path: str, batch_size: int) -> Iterator[DataFrame]:
        """从源提取数据"""
        ...
```

**实现:**
- `S3CSVExtractor`: 从 S3 读取 CSV 文件

#### 3.1.2 Validator (验证器)

**职责:**
- 字段类型验证
- 数据完整性检查
- 业务规则验证
- 生成验证报告

**接口:**
```python
class Validator(Protocol):
    def validate(self, data: DataFrame, schema: Schema) -> ValidationResult:
        """验证数据"""
        # dataframe格式的数据，使用pandera进行验证
        ...
```

**实现:**
- `DuckDBValidator`: 使用 DuckDB 进行数据验证
- 验证规则:
  - 必填字段检查
  - 数据类型检查
  - 唯一性约束
  - 外键约束
  - 业务规则检查

csv schema可参考@doc/env_doc/csv_schema.md
业务规则可参考@doc/env_doc/validation_rules.md

#### 3.1.3 Loader (加载器)

**职责:**
- 批量写入数据库
- 事务管理
- 错误处理和重试
- 性能优化

**接口:**
```python
class Loader(Protocol):
    def load(self, data: List[Model], table_name: str) -> LoadResult:
        """加载数据到目标"""
        ...
```

**实现:**
- `PostgreSQLLoader`: 批量写入 PostgreSQL
- 写入策略:
  - 批量插入 (Batch Insert)
  - UPSERT (INSERT ON CONFLICT)
  - 事务隔离级别控制

### 3.2 数据模型设计

#### 3.2.1 Asset Model (资产表)

```python
class Asset(SQLModel, table=True):
    """资产表模型"""
    id: Optional[int] = Field(default=None, primary_key=True)
    account_id: str = Field(unique=True, index=True, max_length=20)
    account_type: int = Field(index=True)
    cash: Decimal = Field(max_digits=20, decimal_places=2)
    frozen_cash: Decimal = Field(max_digits=20, decimal_places=2)
    market_value: Decimal = Field(max_digits=20, decimal_places=2)
    total_asset: Decimal = Field(max_digits=20, decimal_places=2)
    updated_at: datetime
```

#### 3.2.2 Trade Model (交易表)

```python
class Trade(SQLModel, table=True):
    """交易表模型"""
    id: Optional[int] = Field(default=None, primary_key=True)
    account_id: str = Field(index=True, max_length=20, foreign_key="asset.account_id")
    account_type: int = Field(index=True)
    traded_id: str = Field(unique=True, index=True, max_length=50)
    stock_code: str = Field(index=True, max_length=10)
    traded_time: datetime
    traded_price: Decimal = Field(max_digits=20, decimal_places=2)
    traded_volume: int
    traded_amount: Decimal = Field(max_digits=20, decimal_places=2)
    strategy_name: str = Field(max_length=50)
    order_remark: Optional[str] = Field(default=None, max_length=100)
    direction: int
    offset_flag: int
    created_at: datetime
    updated_at: datetime
```

### 3.3 配置管理架构

使用 Hydra 实现多环境配置:

```
configs/
├── config.yaml           # 主配置
├── db/
│   ├── dev.yaml         # 开发环境数据库
│   ├── test.yaml        # 测试环境数据库
│   └── prod.yaml        # 生产环境数据库
├── s3/
│   ├── dev.yaml         # 开发环境 S3
│   └── prod.yaml        # 生产环境 S3
├── etl/
│   ├── batch_size.yaml  # 批处理配置
│   └── validation.yaml  # 验证规则配置
└── logging/
    └── default.yaml     # 日志配置
```

实际的服务环境配置可参考@doc/env_doc/environment_setup.md

## 4. 数据流设计

### 4.1 完整数据流

```
┌─────────┐
│  Start  │
└────┬────┘
     │
     ▼
┌─────────────────┐
│ Load Config     │ (Hydra)
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Extract from S3 │ (CSV → DataFrame)
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Load to DuckDB  │ (内存验证表)
└────┬────────────┘
     │
     ▼
┌─────────────────┐
│ Validate Data   │
│ - Field types   │
│ - Constraints   │
│ - Business rules│
└────┬────────────┘
     │
     ├──[Valid]────▶┌─────────────────┐
     │              │ Transform Data  │
     │              └────┬────────────┘
     │                   │
     │                   ▼
     │              ┌─────────────────┐
     │              │ Batch Load      │
     │              │ to PostgreSQL   │
     │              └────┬────────────┘
     │                   │
     │                   ▼
     │              ┌─────────────────┐
     │              │ Analytics       │
     │              │ (Statistics)    │
     │              └────┬────────────┘
     │                   │
     │                   ▼
     │              ┌─────────────────┐
     │              │ Success Report  │
     │              └─────────────────┘
     │
     └──[Invalid]──▶┌─────────────────┐
                    │ Error Report    │
                    │ & Logging       │
                    └─────────────────┘
```

### 4.2 批处理流程

为处理大文件，采用批处理策略:

```
S3 CSV File (N rows)
     │
     ▼
┌─────────────────────────────────────┐
│  Stream Read (batch_size=10000)     │
└────┬────────────────────────────────┘
     │
     ├─▶ Batch 1 (rows 1-10000)
     │        │
     │        ├─▶ Validate in DuckDB
     │        └─▶ Load to PostgreSQL
     │
     ├─▶ Batch 2 (rows 10001-20000)
     │        │
     │        ├─▶ Validate in DuckDB
     │        └─▶ Load to PostgreSQL
     │
     └─▶ Batch N ...
              │
              ├─▶ Validate in DuckDB
              └─▶ Load to PostgreSQL
```

## 5. 错误处理策略

### 5.1 错误分类

| 错误类型 | 处理策略 | 示例 |
|----------|----------|------|
| 连接错误 | 重试 3 次，指数退避 | S3 连接失败、DB 连接失败 |
| 验证错误 | 记录错误行，继续处理 | 字段类型错误、约束违反 |
| 业务错误 | 记录日志，跳过该行 | 重复数据、外键不存在 |
| 系统错误 | 立即终止，回滚事务 | 磁盘满、内存溢出 |

### 5.2 错误恢复机制

```python
class ETLPipeline:
    def run(self, checkpoint_enabled: bool = True):
        """
        支持断点续传的 ETL 流程
        """
        # 1. 加载上次 checkpoint
        last_checkpoint = self.load_checkpoint() if checkpoint_enabled else None

        # 2. 从 checkpoint 继续或从头开始
        start_batch = last_checkpoint.batch_id if last_checkpoint else 0

        # 3. 批处理
        for batch_id, batch_data in enumerate(self.extract(), start=start_batch):
            try:
                # 验证
                validation_result = self.validate(batch_data)

                # 加载
                if validation_result.is_valid:
                    self.load(batch_data)

                # 保存 checkpoint
                if checkpoint_enabled:
                    self.save_checkpoint(batch_id)

            except Exception as e:
                self.handle_error(e, batch_id)
```

## 6. 性能优化设计

### 6.1 批量处理

- 批量大小: 10,000 行/批次
- 并行处理: 使用线程池处理多个批次
- 内存控制: 限制同时加载的批次数量

### 6.2 数据库优化

**PostgreSQL 优化:**
- 批量插入 (COPY 或 executemany)
- 索引策略:
  - 主键: `id`
  - 唯一索引: `asset_id`, `trade_id`
  - 普通索引: `account_id`, `timestamp`
- 连接池管理
- 事务批处理

**DuckDB 优化:**
- 内存数据库模式
- 列式存储优化
- 并行查询

### 6.3 缓存策略

- 配置缓存: 启动时加载配置到内存
- Schema 缓存: 缓存数据库 Schema 信息
- 连接池: 复用数据库连接

## 7. 测试架构

### 7.1 测试金字塔

```
        ┌─────────────┐
        │     E2E     │ (5%)
        │   Tests     │
        └─────────────┘
      ┌─────────────────┐
      │   Integration   │ (25%)
      │     Tests       │
      └─────────────────┘
    ┌─────────────────────┐
    │    Unit Tests       │ (70%)
    │                     │
    └─────────────────────┘
```

### 7.2 测试类型

#### 单元测试
- 测试单个函数/类
- Mock 外部依赖
- 快速执行

```python
# tests/unit/test_validator.py
def test_asset_validation():
    validator = DuckDBValidator()
    invalid_asset = Asset(asset_id="", quantity=-1)
    result = validator.validate(invalid_asset)
    assert not result.is_valid
```

#### 集成测试
- 测试组件交互
- 使用测试数据库
- 真实依赖

```python
# tests/integration/test_etl_pipeline.py
def test_full_etl_flow(test_db, s3_mock):
    pipeline = ETLPipeline(config=test_config)
    result = pipeline.run()
    assert result.success
    assert result.rows_processed > 0
```

#### E2E 测试
- 测试完整流程
- 真实环境
- 端到端验证

### 7.3 测试数据管理

```python
# tests/conftest.py
@pytest.fixture
def test_db():
    """测试数据库 fixture"""
    engine = create_engine("postgresql://test:test@localhost/test_db")
    SQLModel.metadata.create_all(engine)
    yield engine
    SQLModel.metadata.drop_all(engine)

@pytest.fixture
def sample_assets():
    """示例资产数据"""
    return [
        Asset(asset_id="A001", account_id="ACC1", ...),
        Asset(asset_id="A002", account_id="ACC2", ...),
    ]
```

## 8. 监控和日志

### 8.1 日志架构

```python
# 日志层级
logging_config = {
    "version": 1,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "logs/etl.log",
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
        },
        "error_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "logs/error.log",
            "level": "ERROR",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["console", "file", "error_file"],
    },
}
```

### 8.2 监控指标

| 指标 | 描述 | 阈值 |
|------|------|------|
| 处理速度 | 行/秒 | > 1000 |
| 错误率 | 错误行数/总行数 | < 1% |
| 内存使用 | MB | < 2GB |
| 批处理时间 | 秒/批次 | < 10s |
| 数据库连接数 | 活跃连接 | < 10 |

### 8.3 告警机制

- 错误率超过阈值
- 处理速度低于预期
- 数据库连接失败
- 磁盘空间不足

## 9. 安全设计

### 9.1 配置安全

- 敏感信息使用环境变量
- 不在代码中硬编码密码
- 使用 AWS IAM 角色访问 S3

```python
# 安全配置示例
database_url = os.getenv("DATABASE_URL")
s3_access_key = os.getenv("AWS_ACCESS_KEY_ID")
s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
```

### 9.2 数据安全

- 传输加密 (TLS/SSL)
- 敏感字段加密存储
- 数据访问日志记录

### 9.3 权限控制

- 数据库用户最小权限原则
- S3 bucket 访问策略
- 审计日志

## 10. 部署架构

### 10.1 部署模式

**开发环境:**
```
Developer Machine
├── Local PostgreSQL (Docker)
├── Local DuckDB
└── AWS S3 (Dev Bucket)
```

**测试环境:**
```
Test Server
├── Test PostgreSQL (Docker)
├── CI/CD Pipeline
└── AWS S3 (Test Bucket)
```

**生产环境:**
```
Production Server
├── AWS RDS PostgreSQL
├── Container (Docker)
└── AWS S3 (Prod Bucket)
```

### 10.2 依赖管理

使用 `pixi` 管理依赖:

```toml
# pixi.toml
[dependencies]
python = "3.11.*"
sqlmodel = ">=0.0.14"
alembic = ">=1.13.0"
duckdb = ">=0.9.0"
boto3 = ">=1.34.0"
hydra-core = ">=1.3.0"
pytest = ">=7.4.0"
```

### 10.3 容器化

```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# 安装 pixi
RUN curl -fsSL https://pixi.sh/install.sh | bash

# 复制项目文件
COPY . /app

# 安装依赖
RUN pixi install

# 运行 ETL
CMD ["pixi", "run", "etl", "--config", "prod"]
```

## 11. 扩展性设计

### 11.1 插件架构

支持自定义 Extractor、Validator、Loader:

```python
class PluginRegistry:
    """插件注册器"""
    extractors: Dict[str, Type[Extractor]] = {}
    validators: Dict[str, Type[Validator]] = {}
    loaders: Dict[str, Type[Loader]] = {}

    @classmethod
    def register_extractor(cls, name: str, extractor: Type[Extractor]):
        cls.extractors[name] = extractor
```

### 11.2 配置驱动

通过配置文件扩展功能:

```yaml
# 自定义 ETL 流程
etl:
  extractors:
    - type: s3_csv
      config:
        bucket: my-bucket
        prefix: data/
  validators:
    - type: duckdb
    - type: custom_business_rule
  loaders:
    - type: postgresql
    - type: parquet_file  # 额外输出 Parquet
```

### 11.3 数据源扩展

预留接口支持更多数据源:

- S3 CSV (已实现)
- S3 Parquet (待扩展)
- HTTP API (待扩展)
- Kafka Stream (待扩展)


## 12. 关键技术决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 验证引擎 | DuckDB | 高性能列式数据库，适合数据验证 |
| ORM | SQLModel | 类型安全，支持 Pydantic 验证 |
| 配置管理 | Hydra | 多环境配置，可组合 |
| 包管理 | pixi | 现代化包管理，跨平台 |
| 数据库迁移 | Alembic | SQLModel 官方推荐 |
