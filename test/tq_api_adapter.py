from tqsdk import TqApi, TqAuth, TqBacktest, BacktestFinished
from tqsdk.objs import Order as TqOrder
from typing import Any, Dict, List, Optional, Union
from api_context import IDataAccessor  # 导入抽象接口

class TqApiAdapter(IDataAccessor):
    """天勤API适配器，实现IDataAccessor接口"""
    def __init__(self, *args, **kwargs):
        self.api = TqApi(*args, **kwargs)
        # 存储backtest引用（如果有）
        self._backtest = kwargs.get('backtest')

    def get_kline_serial(self, symbol: str, duration_seconds: int):
        return self.api.get_kline_serial(symbol, duration_seconds)

    def get_quote(self, symbol: str):
        return self.api.get_quote(symbol)

    # 实现接口方法
    def get_account(self) -> Any:
        return self.api.get_account()

    # 实现接口方法
    def get_position(self, symbol: str) -> Any:
        return self.api.get_position(symbol)

    # 实现接口方法
    def insert_order(self,** kwargs) -> TqOrder:
        return self.api.insert_order(**kwargs)

    def wait_update(self) -> bool:
        return self.api.wait_update()

    # 实现接口方法
    def is_changing(self, obj: Any, fields: Union[str, List[str]]) -> bool:
        return self.api.is_changing(obj, fields)

    def close(self):
        self.api.close()

    @property
    def backtest(self):
        return self._backtest

class TqAuthAdapter:
    """天勤认证适配器"""
    def __init__(self, *args, **kwargs):
        self.auth = TqAuth(*args, **kwargs)

class TqBacktestAdapter:
    """天勤回测适配器"""
    def __init__(self, *args, **kwargs):
        self.backtest = TqBacktest(*args, **kwargs)

BacktestFinishedAdapter = BacktestFinished  # 直接复用异常类