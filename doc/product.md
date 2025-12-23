# 产品引导文档

## 项目目标
- 搭建一个 ETL 系统，实现从 S3 CSV 文件到 PostgreSQL 数据库的同步入库
- 目标数据表：
  - 资产表 (asset表)
  - 交易表 (trade表)
- 实现数据提取、验证、入库、统计分析全流程
- 提供可重复执行、可测试、可扩展的数据管道

## 目标用户
- 主要用户：量化研究员 / 数据工程师
- 使用方式：Python 脚本，支持命令行参数输入
- 使用场景：测试和优化 ETL 管道

## 核心功能
1. 数据提取：从 S3 CSV 文件读取数据（支持 Hydra 配置驱动的格式转换）
2. 数据验证：用 DuckDB + Pandera 进行字段级和业务规则验证
3. 数据入库：批量 UPSERT 到 PostgreSQL（使用 DuckDB PostgreSQL 插件）
4. 数据分析：用 DuckDB 读取已入库数据进行统计分析
5. 命令行界面：argparse 支持 run/assets/trades/clean/schedule 命令
6. 定时任务管理：APScheduler 支持周期性 ETL 执行（PostgreSQL 持久化）
7. Schema 管理：SQLModel + Alembic
8. 配置管理：Hydra + pyproject.toml + pixi
9. 自动化测试：pytest 管理单元测试和集成测试

## 明确不做
- 不处理高频交易和实时流数据
- 不开发 Web 前端或 GUI
- 不直接对接外部交易系统

## 产品原则

### 1. 准确性优先
- 宁可慢一点，也要确保数据完全正确

### 2. 可配置性
- 提供灵活的配置选项
- 支持不同的数据生成场景
- 避免硬编码

### 3. 可测试性
- 重要功能需要有单元测试

### 4. 可维护性
- 代码结构清晰，模块化
- 完善的文档和注释
- 遵循 Python 最佳实践
