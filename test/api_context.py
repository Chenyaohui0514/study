from dataclasses import asdict
from pathlib import Path
import csv
from typing import Dict, Any, List, Optional, Iterable, Protocol, Union
from datetime import datetime
from object import (
    AccountRow, PositionRow, OrderRow, TradeRow,
    ValidationLogRow, WaitUpdateLogRow
)

# 定义抽象数据访问接口
class IDataAccessor(Protocol):
    """数据访问抽象接口，定义数据获取规范"""
    def get_account(self) -> Any:
        ...
    def get_position(self, symbol: str) -> Any:
        ...
    def insert_order(self, **kwargs) -> Any:
        ...
    def is_changing(self, obj: Any, fields: Union[str, List[str]]) -> bool:
        ...

class CsvSink:
    def __init__(self, file: str, headers: Iterable[str]):
        self.path = Path("data") / file
        self.headers = list(headers)
        self.path.parent.mkdir(exist_ok=True)
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()
    def append(self, row: Dict[str, Any]):
        filtered = {k: row.get(k) for k in self.headers}
        with self.path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.headers)
            writer.writerow(filtered)

class ApiContext:
    """API上下文管理类，封装数据落盘和订单管理（与具体API解耦）"""
    def __init__(self, data_accessor: IDataAccessor, gateway_name: str = "tqsdk"):
        self.data_accessor = data_accessor  # 依赖抽象接口
        self.gateway_name = gateway_name
        # 初始化CSV落盘器
        self._init_sinks()
        # 订单管理相关
        self._orders = {}  # order_id -> Order
        self._recent_orders = []  # 记录最近生成的订单用于校验
        self._last_orders_state = {}  # 记录上一次订单状态用于变化检测

    def _init_sinks(self):
        """初始化所有CSV落盘器"""
        self.account_sink = CsvSink(
            "account.csv",
            headers=[f.name for f in AccountRow.__dataclass_fields__.values()]
        )
        self.position_sink = CsvSink(
            "position.csv",
            headers=[f.name for f in PositionRow.__dataclass_fields__.values()] + ["vt_symbol"]
        )
        self.order_sink = CsvSink(
            "order.csv",
            headers=[f.name for f in OrderRow.__dataclass_fields__.values()] + ["vt_symbol", "vt_orderid"]
        )
        self.trade_sink = CsvSink(
            "trade.csv",
            headers=[f.name for f in TradeRow.__dataclass_fields__.values()] + ["vt_symbol", "vt_orderid", "vt_tradeid"]
        )
        self.validation_sink = CsvSink(
            "validation_log.csv",
            headers=[f.name for f in ValidationLogRow.__dataclass_fields__.values()]
        )
        self.wait_update_sink = CsvSink(
            "wait_update_log.csv",
            headers=[f.name for f in WaitUpdateLogRow.__dataclass_fields__.values()]
        )

    # 订单管理方法
    def place_limit(self, symbol: str, direction: str, offset: str, price: float, volume: int):
        o = self.data_accessor.insert_order(
            symbol=symbol,
            direction=direction,
            offset=offset,
            limit_price=price,
            volume=volume
        )
        self._orders[o.order_id] = o
        self._recent_orders.append({
            "order_id": o.order_id,
            "direction": direction,
            "offset": offset,
            "volume": volume,
            "timestamp": datetime.utcnow()
        })
        return o

    def snapshot_orders(self) -> None:
        for oid, o in list(self._orders.items()):
            row = OrderRow(
                gateway_name=self.gateway_name,
                order_id=o.order_id,
                exchange_order_id=getattr(o, "exchange_order_id", None),
                exchange_id=o.exchange_id,
                instrument_id=o.instrument_id,
                direction=o.direction,
                offset=o.offset,
                volume_orign=o.volume_orign,
                volume_left=o.volume_left,
                price_type=o.price_type,
                limit_price=getattr(o, "limit_price", None),
                volume_condition=getattr(o, "volume_condition", None),
                time_condition=getattr(o, "time_condition", None),
                insert_date_time=getattr(o, "insert_date_time", None),
                last_msg=getattr(o, "last_msg", None),
                status=o.status,
                is_dead=getattr(o, "is_dead", None),
                is_online=getattr(o, "is_online", None),
                is_error=getattr(o, "is_error", None),
                trade_price=getattr(o, "trade_price", None),
            ).to_dict()
            self.order_sink.append(row)
            for tid, tr in getattr(o, "trade_records", {}).items():
                trade_row = TradeRow(
                    gateway_name=self.gateway_name,
                    order_id=o.order_id,
                    trade_id=tr.trade_id,
                    exchange_trade_id=getattr(tr, "exchange_trade_id", None),
                    exchange_id=tr.exchange_id,
                    instrument_id=tr.instrument_id,
                    direction=tr.direction,
                    offset=tr.offset,
                    price=tr.price,
                    volume=tr.volume,
                    trade_date_time=tr.trade_date_time,
                ).to_dict()
                self.trade_sink.append(trade_row)

    def get_recent_orders(self, max_age_seconds: int = 300) -> List[Dict]:
        """获取最近指定时间内的订单"""
        now = datetime.utcnow()
        return [
            order for order in self._recent_orders
            if (now - order["timestamp"]).total_seconds() <= max_age_seconds
        ]

    def check_orders_changed(self) -> bool:
        """检查订单是否发生变化"""
        current_state = {oid: (o.status, o.volume_left) for oid, o in self._orders.items()}
        changed = current_state != self._last_orders_state
        self._last_orders_state = current_state.copy()
        return changed

    # 数据落盘方法
    def save_account(self, account) -> None:
        row = AccountRow(
            gateway_name=self.gateway_name,
            currency=getattr(account, "currency", None),
            pre_balance=getattr(account, "pre_balance", None),
            static_balance=getattr(account, "static_balance", None),
            balance=getattr(account, "balance", None),
            available=getattr(account, "available", None),
            ctp_balance=getattr(account, "ctp_balance", None),
            ctp_available=getattr(account, "ctp_available", None),
            float_profit=getattr(account, "float_profit", None),
            position_profit=getattr(account, "position_profit", None),
            close_profit=getattr(account, "close_profit", None),
            frozen_margin=getattr(account, "frozen_margin", None),
            margin=getattr(account, "margin", None),
            frozen_commission=getattr(account, "frozen_commission", None),
            commission=getattr(account, "commission", None),
            frozen_premium=getattr(account, "frozen_premium", None),
            premium=getattr(account, "premium", None),
            deposit=getattr(account, "deposit", None),
            withdraw=getattr(account, "withdraw", None),
            risk_ratio=getattr(account, "risk_ratio", None),
            market_value=getattr(account, "market_value", None),
            user_id=getattr(account, "user_id", None),
        ).to_dict()
        self.account_sink.append(row)

    def save_position(self, position) -> None:
        row = PositionRow(
            gateway_name=self.gateway_name,
            exchange_id=position.exchange_id,
            instrument_id=position.instrument_id,
            pos_long_his=getattr(position, "pos_long_his", None),
            pos_long_today=getattr(position, "pos_long_today", None),
            pos_short_his=getattr(position, "pos_short_his", None),
            pos_short_today=getattr(position, "pos_short_today", None),
            volume_long=getattr(position, "volume_long", None),
            volume_short=getattr(position, "volume_short", None),
            open_price_long=getattr(position, "open_price_long", None),
            open_price_short=getattr(position, "open_price_short", None),
            open_cost_long=getattr(position, "open_cost_long", None),
            open_cost_short=getattr(position, "open_cost_short", None),
            position_price_long=getattr(position, "position_price_long", None),
            position_price_short=getattr(position, "position_price_short", None),
            position_cost_long=getattr(position, "position_cost_long", None),
            position_cost_short=getattr(position, "position_cost_short", None),
            float_profit_long=getattr(position, "float_profit_long", None),
            float_profit_short=getattr(position, "float_profit_short", None),
            float_profit=getattr(position, "float_profit", None),
            position_profit_long=getattr(position, "position_profit_long", None),
            position_profit_short=getattr(position, "position_profit_short", None),
            position_profit=getattr(position, "position_profit", None),
            margin_long=getattr(position, "margin_long", None),
            margin_short=getattr(position, "margin_short", None),
            margin=getattr(position, "margin", None),
            pos=getattr(position, "pos", None),
            pos_long=getattr(position, "pos_long", None),
            pos_short=getattr(position, "pos_short", None),
        ).to_dict()
        self.position_sink.append(row)

    def log_validation(self, log_data: Dict[str, Any]) -> None:
        """记录校验日志"""
        row = ValidationLogRow(
            gateway_name=self.gateway_name,
            event_type=log_data.get("event_type"),
            signal_direction=log_data.get("signal_direction"),
            signal_offset=log_data.get("signal_offset"),
            signal_volume=log_data.get("signal_volume"),
            order_id=log_data.get("order_id"),
            order_direction=log_data.get("order_direction"),
            order_offset=log_data.get("order_offset"),
            order_volume=log_data.get("order_volume"),
            status=log_data.get("status"),
            message=log_data.get("message"),
        ).to_dict()
        self.validation_sink.append(row)

    def log_wait_update(self, log_data: Dict[str, Any]) -> None:
        """记录wait_update校验日志"""
        row = WaitUpdateLogRow(
            gateway_name=self.gateway_name,
            update_id=log_data.get("update_id"),
            duration_ms=log_data.get("duration_ms"),
            has_changes=log_data.get("has_changes"),
            d1_changed=log_data.get("d1_changed"),
            m1_changed=log_data.get("m1_changed"),
            account_changed=log_data.get("account_changed"),
            position_changed=log_data.get("position_changed"),
            orders_changed=log_data.get("orders_changed"),
            tqsdk_status=log_data.get("tqsdk_status"),
            validation_status=log_data.get("validation_status"),
            message=log_data.get("message"),
        ).to_dict()
        self.wait_update_sink.append(row)