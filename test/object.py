from dataclasses import dataclass, field, asdict
from datetime import datetime, date as DateClass
from typing import Optional, Dict, Any, Iterable
import os, csv
from pathlib import Path

@dataclass
class BaseData:
    gateway_name: str
    extra: Optional[dict] = field(default=None, init=False)
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class AccountRow(BaseData):
    currency: Optional[str] = None
    pre_balance: Optional[float] = None
    static_balance: Optional[float] = None
    balance: Optional[float] = None
    available: Optional[float] = None
    ctp_balance: Optional[float] = None
    ctp_available: Optional[float] = None
    float_profit: Optional[float] = None
    position_profit: Optional[float] = None
    close_profit: Optional[float] = None
    frozen_margin: Optional[float] = None
    margin: Optional[float] = None
    frozen_commission: Optional[float] = None
    commission: Optional[float] = None
    frozen_premium: Optional[float] = None
    premium: Optional[float] = None
    deposit: Optional[float] = None
    withdraw: Optional[float] = None
    risk_ratio: Optional[float] = None
    market_value: Optional[float] = None
    user_id: Optional[str] = None
    asset: Optional[float] = None
    asset_his: Optional[float] = None
    available_his: Optional[float] = None
    cost: Optional[float] = None
    drawable: Optional[float] = None
    buy_frozen_balance: Optional[float] = None
    buy_frozen_fee: Optional[float] = None
    buy_balance_today: Optional[float] = None
    buy_fee_today: Optional[float] = None
    sell_balance_today: Optional[float] = None
    sell_fee_today: Optional[float] = None
    hold_profit: Optional[float] = None
    float_profit_today: Optional[float] = None
    real_profit_today: Optional[float] = None
    profit_today: Optional[float] = None
    profit_rate_today: Optional[float] = None
    dividend_balance_today: Optional[float] = None
    # 时间格式改为包含微秒的精细时间
    ts: str = field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

@dataclass
class SubAccountRow(BaseData):
    subaccount_id: str
    user_id: Optional[str] = None
    currency: Optional[str] = None
    balance: Optional[float] = None
    available: Optional[float] = None
    margin: Optional[float] = None
    market_value: Optional[float] = None
    # 时间格式改为包含微秒的精细时间
    ts: str = field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

@dataclass
class PositionRow(BaseData):
    exchange_id: str
    instrument_id: str
    pos_long_his: Optional[int] = None
    pos_long_today: Optional[int] = None
    pos_short_his: Optional[int] = None
    pos_short_today: Optional[int] = None
    volume_long: Optional[int] = None
    volume_short: Optional[int] = None
    open_price_long: Optional[float] = None
    open_price_short: Optional[float] = None
    open_cost_long: Optional[float] = None
    open_cost_short: Optional[float] = None
    position_price_long: Optional[float] = None
    position_price_short: Optional[float] = None
    position_cost_long: Optional[float] = None
    position_cost_short: Optional[float] = None
    float_profit_long: Optional[float] = None
    float_profit_short: Optional[float] = None
    float_profit: Optional[float] = None
    position_profit_long: Optional[float] = None
    position_profit_short: Optional[float] = None
    position_profit: Optional[float] = None
    margin_long: Optional[float] = None
    margin_short: Optional[float] = None
    margin: Optional[float] = None
    pos: Optional[int] = None
    pos_long: Optional[int] = None
    pos_short: Optional[int] = None
    # 时间格式改为包含微秒的精细时间
    ts: str = field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["vt_symbol"] = f"{self.instrument_id}.{self.exchange_id}"
        return d

@dataclass
class OrderRow(BaseData):
    order_id: str
    exchange_order_id: Optional[str]
    exchange_id: str
    instrument_id: str
    direction: str           # BUY/SELL
    offset: str              # OPEN/CLOSE/CLOSETODAY
    volume_orign: int
    volume_left: int
    price_type: str          # ANY/LIMIT
    limit_price: Optional[float]
    volume_condition: Optional[str] = None
    time_condition: Optional[str] = None
    insert_date_time: Optional[int] = None  # ns
    last_msg: Optional[str] = None
    status: Optional[str] = None            # ALIVE/FINISHED
    is_dead: Optional[bool] = None
    is_online: Optional[bool] = None
    is_error: Optional[bool] = None
    trade_price: Optional[float] = None
    # 时间格式改为包含微秒的精细时间
    ts: str = field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["vt_symbol"] = f"{self.instrument_id}.{self.exchange_id}"
        d["vt_orderid"] = f"{self.gateway_name}.{self.order_id}"
        return d

@dataclass
class TradeRow(BaseData):
    order_id: str
    trade_id: str
    exchange_trade_id: Optional[str]
    exchange_id: str
    instrument_id: str
    direction: str           # BUY/SELL
    offset: str              # OPEN/CLOSE/CLOSETODAY
    price: float
    volume: int
    trade_date_time: int     # ns
    # 时间格式改为包含微秒的精细时间
    ts: str = field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["vt_symbol"] = f"{self.instrument_id}.{self.exchange_id}"
        d["vt_orderid"] = f"{self.gateway_name}.{self.order_id}"
        d["vt_tradeid"] = f"{self.gateway_name}.{self.trade_id}"
        return d

@dataclass
class ValidationLogRow(BaseData):
    """校验日志数据行"""
    event_type: Optional[str] = None
    signal_direction: Optional[str] = None
    signal_offset: Optional[str] = None
    signal_volume: Optional[int] = None
    order_id: Optional[str] = None
    order_direction: Optional[str] = None
    order_offset: Optional[str] = None
    order_volume: Optional[int] = None
    status: Optional[str] = None
    message: Optional[str] = None
    # 时间格式改为包含微秒的精细时间
    ts: str = field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))

@dataclass
class WaitUpdateLogRow(BaseData):
    """WaitUpdate校验日志数据行"""
    update_id: Optional[int] = None
    duration_ms: Optional[float] = None
    has_changes: Optional[bool] = None
    d1_changed: Optional[bool] = None
    m1_changed: Optional[bool] = None
    account_changed: Optional[bool] = None
    position_changed: Optional[bool] = None
    orders_changed: Optional[bool] = None
    tqsdk_status: Optional[str] = None
    validation_status: Optional[str] = None
    message: Optional[str] = None
    # 时间格式改为包含微秒的精细时间
    ts: str = field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))