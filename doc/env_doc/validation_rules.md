## 1. 数据验证规则

### 1.1 字段级验证
- **金额字段** (cash, frozen_cash, market_value, total_asset, traded_price, traded_amount):
  - 类型: Decimal
  - 范围: ≥ 0
  - 精度: 保留2位小数
  - 禁止使用 float 类型

- **数量字段** (traded_volume):
  - 类型: Integer
  - 范围: > 0
  - 股票: 必须是100的倍数

- **时间字段** (updated_at, traded_time, created_at):
  - 格式: ISO 8601 (YYYY-MM-DDTHH:mm:ss)
  - 时区: 统一使用 UTC

- **枚举字段** (account_type, offset_flag):
  - 必须在允许的枚举值范围内

### 1.2 业务级验证
- **AccountAsset**:
  - total_asset = cash + frozen_cash + market_value（允许1分钱误差）

- **Trade**:
  - traded_amount = traded_price * traded_volume（允许1分钱误差）
  - account_id 必须在 AccountAsset 中存在
  - traded_time ≤ updated_at

### 1.3 数据完整性校验
- **外键约束**: Trade.account_id 必须存在于 AccountAsset.account_id
- **计算字段校验**:
  - `total_asset = cash + frozen_cash + market_value` (允许浮点数精度误差 ±0.01)
  - `traded_amount = traded_price * traded_volume` (允许浮点数精度误差 ±0.01)
