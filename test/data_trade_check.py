# -*- coding: utf-8 -*-
"""数据交易校验模块"""
from typing import List, Dict, Callable, Optional
from datetime import datetime
from object import ValidationLogRow, WaitUpdateLogRow
from api_context import IDataAccessor  # 导入抽象接口

class StrategyValidator:
    """策略信号与订单执行校验器"""
    def __init__(self):
        self._expected_signals = []  # 预期信号列表
        self._log_callback = None    # 日志回调函数

    def set_log_callback(self, callback: Callable[[Dict], None]):
        """设置日志回调函数"""
        self._log_callback = callback

    def record_expected_signal(self, direction: str, offset: str, volume: int):
        """记录预期的交易信号"""
        self._expected_signals.append({
            "direction": direction,
            "offset": offset,
            "volume": volume,
            "timestamp": datetime.now()
        })

    def validate_orders(self, recent_orders: List[Dict]):
        """校验最近订单与预期信号是否匹配"""
        if not self._expected_signals or not recent_orders:
            return
        # 遍历预期信号，检查是否有对应的订单
        for signal in self._expected_signals[:]:  # 复制列表以支持删除
            matched = False
            for order in recent_orders:
                if (signal["direction"] == order["direction"] and
                    signal["offset"] == order["offset"] and
                    signal["volume"] == order["volume"]):
                    # 找到匹配的订单
                    matched = True
                    if self._log_callback:
                        self._log_callback({
                            "event_type": "ORDER_VALIDATION",
                            "signal_direction": signal["direction"],
                            "signal_offset": signal["offset"],
                            "signal_volume": signal["volume"],
                            "order_id": order["order_id"],
                            "order_direction": order["direction"],
                            "order_offset": order["offset"],
                            "order_volume": order["volume"],
                            "status": "MATCHED",
                            "message": f"Signal matched order {order['order_id']}"
                        })
                    self._expected_signals.remove(signal)
                    break
            if not matched:
                # 未找到匹配的订单
                if self._log_callback:
                    self._log_callback({
                        "event_type": "ORDER_VALIDATION",
                        "signal_direction": signal["direction"],
                        "signal_offset": signal["offset"],
                        "signal_volume": signal["volume"],
                        "order_id": None,
                        "order_direction": None,
                        "order_offset": None,
                        "order_volume": None,
                        "status": "MISMATCH",
                        "message": "Expected signal not matched by any order"
                    })

class WaitUpdateValidator:
    """WaitUpdate数据一致性校验器"""
    def __init__(self, api_adapter: IDataAccessor, d1, m1, account, position, order_manager):
        self.api_adapter = api_adapter  # 依赖抽象接口
        self.d1 = d1
        self.m1 = m1
        self.account = account
        self.position = position
        self.order_manager = order_manager
        self._log_callback = None
        self._last_d1_dt = None
        self._last_m1_dt = None
        self._last_account_balance = None
        self._last_position_pos = None
        self._last_orders_state = None
        self._update_counter = 0

    def set_log_callback(self, callback: Callable[[Dict], None]):
        """设置日志回调函数"""
        self._log_callback = callback

    def validate(self) -> bool:
        """执行校验并返回是否通过"""
        self._update_counter += 1
        start_time = datetime.now()
        # 等待数据更新
        try:
            update_result = self.api_adapter.wait_update()
        except Exception as e:
            if self._log_callback:
                self._log_callback({
                    "update_id": self._update_counter,
                    "duration_ms": 0,
                    "has_changes": False,
                    "d1_changed": False,
                    "m1_changed": False,
                    "account_changed": False,
                    "position_changed": False,
                    "orders_changed": False,
                    "tqsdk_status": "ERROR",
                    "validation_status": "FAIL",
                    "message": f"wait_update failed: {str(e)}"
                })
            return False
        # 计算耗时
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        # 检查各数据是否变化
        d1_changed = self._check_d1_changed()
        m1_changed = self._check_m1_changed()
        account_changed = self._check_account_changed()
        position_changed = self._check_position_changed()
        orders_changed = self.order_manager.check_orders_changed()
        has_changes = (d1_changed or m1_changed or account_changed or
                      position_changed or orders_changed)
        # 记录日志
        if self._log_callback:
            self._log_callback({
                "update_id": self._update_counter,
                "duration_ms": round(duration_ms, 2),
                "has_changes": has_changes,
                "d1_changed": d1_changed,
                "m1_changed": m1_changed,
                "account_changed": account_changed,
                "position_changed": position_changed,
                "orders_changed": orders_changed,
                "tqsdk_status": "OK" if update_result else "NO_UPDATE",
                "validation_status": "PASS",
                "message": "Validation passed"
            })
        return update_result

    def _check_d1_changed(self) -> bool:
        """检查日线数据是否变化"""
        current_dt = self.d1.datetime.iloc[-1] if len(self.d1) > 0 else None
        changed = current_dt != self._last_d1_dt
        self._last_d1_dt = current_dt
        return changed

    def _check_m1_changed(self) -> bool:
        """检查1分钟线数据是否变化"""
        current_dt = self.m1.datetime.iloc[-1] if len(self.m1) > 0 else None
        changed = current_dt != self._last_m1_dt
        self._last_m1_dt = current_dt
        return changed

    def _check_account_changed(self) -> bool:
        """检查账户数据是否变化"""
        current_balance = getattr(self.account, "balance", None)
        changed = current_balance != self._last_account_balance
        self._last_account_balance = current_balance
        return changed

    def _check_position_changed(self) -> bool:
        """检查持仓数据是否变化"""
        current_pos = getattr(self.position, "pos", None)
        changed = current_pos != self._last_position_pos
        self._last_position_pos = current_pos
        return changed