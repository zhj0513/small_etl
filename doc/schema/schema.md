# -----------资产表(asset)-------------------------------
| 字段名          | 类型          | 说明                      |
| ------------ | ----------- | ----------------------- |
| id           | bigint / 主键 | 表内唯一 ID                 |
| account_id   | varchar     | 资金账号, 例如 `'1000000365'` |
| account_type | varchar     | 账号类型（枚举值）                      |
| cash         | decimal     | 可用金额（可下单使用的金额）    |
| frozen_cash  | decimal     | 冻结金额（挂单、占用资金等）    |
| market_value | decimal     | 持仓市值（当前持仓的市值）      |
| total_asset  | decimal     | 总资产 = cash + frozen_cash + market_value     |
| updated_at   | timestamptz    | 更新时间                    |
-------------------------------------------------------------


# -----------交易表(trade)---------------------------------------
| 字段名           | 类型          | 说明                      |
| ------------- | ----------- | -------------------------- |
| id            | bigint / 主键 | 表内唯一 ID                  |
| account_id    | varchar     | 资金账号                       |
| account_type  | varchar     | 账号类型（枚举值）                       |
| traded_id     | varchar     | 成交编号                       |
| stock_code    | varchar     | 证券代码                       |
| traded_time   | timestamptz | 成交时间                       |
| traded_price  | decimal     | 成交均价                       |
| traded_volume | int         | 成交数量                       |
| traded_amount | decimal     | 成交金额                       |
| strategy_name | varchar     | 策略名称                       |
| order_remark  | varchar     | 下单备注                       |
| direction     | varchar     | 多空方向（枚举值，股票不适用,默认为''）   |
| offset_flag   | varchar     | 操作标识（枚举值，用此字段区分股票买卖，期货开、平仓，期权买卖等）                      |
| created_at    | timestamptz    | 记录创建时间                   |
| updated_at    | timestamptz    | 更新时间                       |
------------------------------------------------------------------

---------------------------------------------------------------------------
# 资产表、交易表中枚举字段的枚举值，可参考xtconstants_250516.1.1.json中的映射关系
账号类型(account_type)
    枚举变量名	值	含义
    FUTURE_ACCOUNT   1   期货
    SECURITY_ACCOUNT 2   股票
    CREDIT_ACCOUNT   3   信用
    FUTURE_OPTION_ACCOUNT  5  期货期权
    STOCK_OPTION_ACCOUNT  6  股票期权
    HUGANGTONG_ACCOUNT  7  沪港通
    SHENGANGTONG_ACCOUNT  11  深港通

多空方向(direction)  # 股票不适用
    枚举变量名	值	含义
    DIRECTION_FLAG_LONG	48	多
    DIRECTION_FLAG_SHORT	49	空

交易操作(offset_flag)
    枚举变量名	值	含义
    OFFSET_FLAG_OPEN	48	买入，开仓
    OFFSET_FLAG_CLOSE	49	卖出，平仓
    OFFSET_FLAG_FORCECLOSE	50	强平
    OFFSET_FLAG_CLOSETODAY	51	平今
    OFFSET_FLAG_ClOSEYESTERDAY	52	平昨
    OFFSET_FLAG_FORCEOFF	53	强减
    OFFSET_FLAG_LOCALFORCECLOSE	54	本地强平
---------------------------------------------------------------------------
