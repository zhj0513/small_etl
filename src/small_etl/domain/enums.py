"""Enum definitions for Small ETL domain."""

from enum import IntEnum


class AccountType(IntEnum):
    """Account type enumeration.

    Based on xtconstants mapping.
    """

    FUTURE = 1  # 期货账户
    SECURITY = 2  # 股票账户
    CREDIT = 3  # 信用账户
    FUTURE_OPTION = 5  # 期货期权账户
    STOCK_OPTION = 6  # 股票期权账户
    HUGANGTONG = 7  # 沪港通账户
    SHENGANGTONG = 11  # 深港通账户


class OffsetFlag(IntEnum):
    """Offset flag enumeration for trade operations.

    ASCII code values for compatibility with trading systems.
    """

    OPEN = 48  # 买入/开仓 (ASCII '0')
    CLOSE = 49  # 卖出/平仓 (ASCII '1')
    FORCECLOSE = 50  # 强平 (ASCII '2')
    CLOSETODAY = 51  # 平今 (ASCII '3')
    CLOSEYESTERDAY = 52  # 平昨 (ASCII '4')
    FORCEOFF = 53  # 强减 (ASCII '5')
    LOCALFORCECLOSE = 54  # 本地强平 (ASCII '6')


class Direction(IntEnum):
    """Direction flag enumeration for futures trading.

    Stock trades use NA (0) as direction is not applicable.
    """

    NA = 0  # 不适用（股票）
    LONG = 48  # 多 (ASCII '0')
    SHORT = 49  # 空 (ASCII '1')


# Valid values sets for validation
VALID_ACCOUNT_TYPES = frozenset(t.value for t in AccountType)
VALID_OFFSET_FLAGS = frozenset(f.value for f in OffsetFlag)
VALID_DIRECTIONS = frozenset(d.value for d in Direction)
