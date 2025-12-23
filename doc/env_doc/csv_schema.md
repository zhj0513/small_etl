# CSV 数据格式规范

本文档定义了 ETL 系统处理的两个 CSV 文件的数据格式规范。

## 1. 资产表 (AccountAsset)

### 1.1 文件说明
- **文件名**: `account_assets.csv`
- **编码**: UTF-8
- **分隔符**: 逗号 (`,`)
- **表头**: 必须包含
- **用途**: 记录账户资产信息，每个账号对应一条资产记录

### 1.2 字段定义

| 字段名 | 数据类型 | 必填 | 说明 | 示例 | 约束 |
|--------|----------|------|------|------|------|
| id | Integer | 是 | 主键ID | 1 | 唯一，自增 |
| account_id | String | 是 | 账户ID | ACC001 | 唯一标识，建议索引 |
| account_type | String | 是 | 账户类型 | 1 | 见账户类型枚举 |
| cash | Decimal | 是 | 可用资金 | 100000.00 | ≥0，保留2位小数 |
| frozen_cash | Decimal | 是 | 冻结资金 | 5000.00 | ≥0，保留2位小数 |
| market_value | Decimal | 是 | 持仓市值 | 200000.00 | ≥0，保留2位小数 |
| total_asset | Decimal | 是 | 总资产 | 305000.00 | ≥0，保留2位小数，= cash + frozen_cash + market_value |
| updated_at | DateTime | 是 | 更新时间 | 2025-12-22T14:30:00 | ISO 8601 格式 |

### 1.3 账户类型枚举 (account_type)

| 代码 | 说明 | 备注 |
|------|------|------|
| 1 | 期货账户 (FUTURE_ACCOUNT) | 期货交易账户 |
| 2 | 股票账户 (SECURITY_ACCOUNT) | 最常见，占比最高 |
| 3 | 信用账户 (CREDIT_ACCOUNT) | 融资融券账户 |
| 5 | 期货期权账户 (FUTURE_OPTION_ACCOUNT) | 期货期权 |
| 6 | 股票期权账户 (STOCK_OPTION_ACCOUNT) | 股票期权 |
| 7 | 沪港通账户 (HUGANGTONG_ACCOUNT) | 沪港通 |
| 11 | 深港通账户 (SHENGANGTONG_ACCOUNT) | 深港通 |

### 1.4 数据生成规则
- **account_id**: 格式为 `1000XXXXXX`（10位数字，从100000001开始递增）
- **account_type**: 主要使用 2（股票账户，占比60%），其次是 3（信用账户，20%）和 1（期货账户，20%）
- **cash**: 随机范围 [10,000, 1,000,000]
- **frozen_cash**: 随机范围 [0, cash * 0.3]
- **market_value**: 随机范围 [0, cash * 2]
- **total_asset**: 计算值 = cash + frozen_cash + market_value
- **updated_at**: 当前时间 - 随机 [0, 60] 分钟

### 1.5 CSV 示例

```csv
id,account_id,account_type,cash,frozen_cash,market_value,total_asset,updated_at
1,10000000001,2,100000.00,5000.00,200000.00,305000.00,2025-12-22T14:30:00
2,10000000002,2,500000.00,50000.00,800000.00,1350000.00,2025-12-22T14:25:00
3,10000000003,3,250000.00,75000.00,300000.00,625000.00,2025-12-22T14:20:00
4,10000000004,1,80000.00,10000.00,120000.00,210000.00,2025-12-22T14:15:00
```

---

## 2. 交易表 (Trade)

### 2.1 文件说明
- **文件名**: `trades.csv`
- **编码**: UTF-8
- **分隔符**: 逗号 (`,`)
- **表头**: 必须包含
- **用途**: 记录交易明细，与 AccountAsset 通过 account_id 关联

### 2.2 字段定义

| 字段名 | 数据类型 | 必填 | 说明 | 示例 | 约束 |
|--------|----------|------|------|------|------|
| id | Integer | 是 | 主键ID | 1 | 唯一，自增 |
| account_id | String | 是 | 账户ID | ACC001 | 外键关联 AccountAsset.account_id |
| account_type | String | 是 | 账户类型 | 1 | 见账户类型枚举（主要是1和2） |
| traded_id | String | 是 | 交易流水号 | T20251222000001 | 唯一，格式：T+日期+序号 |
| stock_code | String | 是 | 股票代码 | 600000 | 6位数字 |
| traded_time | DateTime | 是 | 成交时间 | 2025-12-22T10:30:00 | ISO 8601 格式 |
| traded_price | Decimal | 是 | 成交价格 | 15.50 | >0，保留2位小数 |
| traded_volume | Integer | 是 | 成交数量 | 1000 | >0，股票为100的倍数 |
| traded_amount | Decimal | 是 | 成交金额 | 15500.00 | >0，保留2位小数，= traded_price * traded_volume |
| strategy_name | String | 是 | 策略名称 | 策略A | 见策略名称列表 |
| order_remark | String | 否 | 订单备注 | 正常交易 | 可为空，见备注列表 |
| direction | Integer | 是 | 多空方向 | 0 | 股票固定为0（不适用） |
| offset_flag | Integer | 是 | 开平标识 | 1 | 见开平标识枚举 |
| created_at | DateTime | 是 | 创建时间 | 2025-12-22T10:30:00 | ISO 8601 格式 |
| updated_at | DateTime | 是 | 更新时间 | 2025-12-22T14:30:00 | ISO 8601 格式 |

### 2.3 开平标识枚举 (offset_flag)

| 代码 | 说明 | 备注 |
|------|------|------|
| 48 | 开仓/买入 (OFFSET_FLAG_OPEN) | 建立新仓位，ASCII码对应字符'0' |
| 49 | 平仓/卖出 (OFFSET_FLAG_CLOSE) | 关闭现有仓位，ASCII码对应字符'1' |

### 2.4 策略名称列表 (strategy_name)

- 策略A
- 策略B
- 策略C
- 量化1号
- 量化2号
- 高频策略
- 趋势跟踪

### 2.5 订单备注列表 (order_remark)

- 正常交易
- 网格交易
- 止盈
- 止损
- 建仓
- 平仓
- (空字符串)

### 2.6 数据生成规则
- **account_id**: 从生成的账户ID列表中随机选择（格式：1000XXXXXX）
- **account_type**: 主要使用 2（股票账户，占比75%）和 3（信用账户，25%）
- **traded_id**: 格式为 `T{YYYYMMDD}{序号6位}`，例如：T20251222000001
- **stock_code**: 6位股票代码，包含：
  - 上海市场：600xxx, 601xxx, 603xxx
  - 深圳市场：000xxx, 002xxx, 300xxx
- **traded_price**: 随机范围 [5.0, 200.0]
- **traded_volume**: 随机 [1, 50] * 100（股票以100股为单位）
- **traded_amount**: 计算值 = traded_price * traded_volume
- **direction**: 股票固定为 0（不适用多空方向）
- **offset_flag**: 48（买入）或 49（卖出）随机选择
- **traded_time**: 当前时间 - 随机 [0, 48] 小时
- **created_at**: 当前时间 - 随机 [0, 48] 小时
- **updated_at**: 当前时间 - 随机 [0, 60] 分钟

### 2.7 CSV 示例

```csv
id,account_id,account_type,traded_id,stock_code,traded_time,traded_price,traded_volume,traded_amount,strategy_name,order_remark,direction,offset_flag,created_at,updated_at
1,10000000001,2,T20251222000001,600000,2025-12-22T10:30:00,15.50,1000,15500.00,策略A,正常交易,0,48,2025-12-22T10:30:00,2025-12-22T14:30:00
2,10000000002,2,T20251222000002,000001,2025-12-22T11:15:00,28.30,500,14150.00,量化1号,建仓,0,48,2025-12-22T11:15:00,2025-12-22T14:25:00
3,10000000001,3,T20251222000003,600036,2025-12-22T13:45:00,42.80,2000,85600.00,趋势跟踪,止盈,0,49,2025-12-22T13:45:00,2025-12-22T14:20:00
4,10000000003,2,T20251222000004,300750,2025-12-22T09:30:00,105.20,300,31560.00,高频策略,网格交易,0,48,2025-12-22T09:30:00,2025-12-22T14:15:00
```

---

## 3. 数据关系

### 3.1 表关系
- **Trade.account_id** 外键关联 **AccountAsset.account_id**
- 关系类型：多对一（多个交易记录对应一个资产账户）

### 3.2 数据一致性约束
1. Trade 表中的 account_id 必须在 AccountAsset 表中存在
2. Trade 表中的 account_type 应与对应 AccountAsset 的 account_type 一致
3. traded_amount 必须等于 traded_price * traded_volume
4. total_asset 必须等于 cash + frozen_cash + market_value

### 3.3 导入顺序
1. **优先导入**: AccountAsset（父表）
2. **后续导入**: Trade（子表，依赖 AccountAsset）


## 4. 常量定义参考

以下是生成代码中使用的常量，供验证器实现参考：

```python
# 账户类型（来自 xtconstants_250516.1.1.json）
ACCOUNT_TYPES = {
    'FUTURE_ACCOUNT': 1,           # 期货账户
    'SECURITY_ACCOUNT': 2,         # 股票账户
    'CREDIT_ACCOUNT': 3,           # 信用账户
    'FUTURE_OPTION_ACCOUNT': 5,    # 期货期权账户
    'STOCK_OPTION_ACCOUNT': 6,     # 股票期权账户
    'HUGANGTONG_ACCOUNT': 7,       # 沪港通账户
    'SHENGANGTONG_ACCOUNT': 11     # 深港通账户
}

# 股票开平标识（ASCII码形式）
STOCK_OFFSET_FLAGS = {
    'OFFSET_FLAG_OPEN': 48,   # 买入/开仓（对应字符'0'）
    'OFFSET_FLAG_CLOSE': 49   # 卖出/平仓（对应字符'1'）
}

# 多空方向标识（股票不使用，期货使用）
DIRECTION_FLAGS = {
    'DIRECTION_FLAG_LONG': 48,   # 多（对应字符'0'）
    'DIRECTION_FLAG_SHORT': 49   # 空（对应字符'1'）
}

# 策略名称列表
STRATEGY_NAMES = [
    "策略A", "策略B", "策略C",
    "量化1号", "量化2号",
    "高频策略", "趋势跟踪"
]

# 订单备注列表
ORDER_REMARKS = [
    "正常交易", "网格交易", "止盈",
    "止损", "建仓", "平仓", ""
]
```
