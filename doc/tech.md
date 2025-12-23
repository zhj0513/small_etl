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
- CLI：argparse（标准库命令行解析）
- 任务调度：APScheduler（定时任务管理，PostgreSQL 持久化）
- 静态检查：pyright, pyrefly, ruff
- 测试框架：pytest

## 数据管道
- CSV 数据来源：S3（可通过 MinIO 模拟）
- 数据提取：ExtractorService 用 Hydra 管理 CSV 格式定义，使用 Polars 进行格式转换
- 数据验证：DuckDB 导入 + Pandera 进行字段级和业务规则校验
- 数据写入：LoaderService 使用 DuckDB 的 PostgreSQL 插件进行批量 UPSERT
- 数据分析：AnalyticsService 用 DuckDB 读取已入库数据进行统计分析

## 必须遵守的约束
- 资产表、交易表必须用 SQLModel 定义
- 数据库操作的所有类必须有单元测试
- 金额相关的字段禁止使用 float ，统一使用 Decimal
- 所有时间字段统一使用 UTC
- 所有配置集中在 pyproject.toml 与 Hydra

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
