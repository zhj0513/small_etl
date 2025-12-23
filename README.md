# Small ETL

S3 CSV 文件到 PostgreSQL 的批量数据 ETL 系统。

## 快速开始

```bash
# 安装依赖
pixi install

# 运行完整 ETL 流程
pixi run python -m small_etl run
```

## CLI 命令

### 基本用法

```bash
# 完整 ETL 流程 (assets + trades)
pixi run python -m small_etl run

# 仅处理 Assets
pixi run python -m small_etl assets

# 仅处理 Trades (需要 assets 已加载)
pixi run python -m small_etl trades

# 清空数据表
pixi run python -m small_etl clean
```

### 常用参数

| 参数 | 短选项 | 说明 | 默认值 |
|------|--------|------|--------|
| `--env` | `-e` | 环境配置 (dev/test) | dev |
| `--batch-size` | `-b` | 批处理大小 | 10000 |
| `--verbose` | `-v` | 详细输出 | False |
| `--dry-run` | | 仅验证不加载 | False |

### 使用示例

```bash
# 使用测试环境配置
pixi run python -m small_etl run --env test

# 详细输出模式
pixi run python -m small_etl run --verbose

# 仅验证数据，不写入数据库
pixi run python -m small_etl run --dry-run

# 自定义批处理大小
pixi run python -m small_etl run --batch-size 5000

# 清空测试环境数据
pixi run python -m small_etl clean --env test
```

### 覆盖配置

**S3 配置:**

```bash
pixi run python -m small_etl run \
    --s3-endpoint localhost:9000 \
    --s3-bucket my-bucket \
    --assets-file data/assets.csv \
    --trades-file data/trades.csv
```

**数据库配置:**

```bash
pixi run python -m small_etl run \
    --db-host 192.168.1.100 \
    --db-port 5432 \
    --db-name mydb \
    --db-user admin \
    --db-password secret
```

### 退出码

| 退出码 | 含义 |
|--------|------|
| 0 | 成功 |
| 1 | 一般错误 |
| 2 | 参数错误 |
| 3 | 连接错误 (S3/DB) |
| 4 | 验证错误 (有无效数据) |

## 数据库迁移

```bash
# 生成迁移脚本
pixi run alembic revision --autogenerate -m "description"

# 执行迁移
pixi run alembic upgrade head

# 测试环境迁移
ETL_ENV=test pixi run alembic upgrade head
```

## 开发

```bash
# 运行单元测试
pixi run pytest tests/unit/ -v --no-cov

# 运行集成测试 (需要 PostgreSQL)
pixi run pytest tests/integration/ -v --no-cov

# 代码检查
pixi run ruff check src/
pixi run pyright src/
```

## 架构

详见 [doc/architecture.md](doc/architecture.md)
