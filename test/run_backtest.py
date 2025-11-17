# -*- coding: utf-8 -*-
import math
from typing import Optional
import pandas as pd
from datetime import date as DateClass
from tqsdk import TqBacktest, TqAuth, TqApi, BacktestFinished
from tqsdk.ta import ATR
from tq_api_adapter import TqApiAdapter, TqAuthAdapter, TqBacktestAdapter, BacktestFinishedAdapter
from api_context import ApiContext
from data_trade_check import StrategyValidator, WaitUpdateValidator

# —— 策略参数 ——（可改）
SYMBOL = "CFFEX.IC2306"
POSITION_SIZE = 30
START_DATE = DateClass(2022, 11, 1)
END_DATE = DateClass(2023, 4, 30)
SHORT_TERM_MA = 5  # 短期均线周期
LONG_TERM_MA = 20  # 长期均线周期
ATR_PERIOD = 14  # ATR周期
STOP_LOSS_MULTIPLIER = 2.0  # 止损倍数
TAKE_PROFIT_MULTIPLIER = 3.0  # 止盈倍数

def run_strategy():
    print("开始运行均线交叉期货策略(含表采集和实时对比)...")
    # 初始化回测和API适配器（实现IDataAccessor接口）
    backtest = TqBacktest(start_dt=START_DATE, end_dt=END_DATE)
    auth = TqAuth("18003940514", "Cyh020508")
    data_accessor = TqApiAdapter(backtest=backtest, auth=auth)  # 接口实现类
    context = ApiContext(data_accessor, gateway_name="tqsdk")  # 注入依赖

    # —— 订阅数据 ——：日线（生成信号）+ 1分钟（触发挂起订单）
    d1 = data_accessor.get_kline_serial(SYMBOL, 60 * 60 * 24)  # 日线
    m1 = data_accessor.get_kline_serial(SYMBOL, 60)  # 1分钟，用于次日第一分钟触发
    quote = data_accessor.get_quote(SYMBOL)  # 备用拿最新价

    # 初始化校验器
    validator = StrategyValidator()
    validator.set_log_callback(context.log_validation)
    wait_update_validator = WaitUpdateValidator(
        data_accessor, d1, m1, data_accessor.get_account(), data_accessor.get_position(SYMBOL), context
    )
    wait_update_validator.set_log_callback(context.log_wait_update)

    account = data_accessor.get_account()
    position = data_accessor.get_position(SYMBOL)

    # 日线切换判定
    last_bar_dt = None  # ns
    # —— 状态变量 ——
    current_direction = 0  # 1=多 -1=空 0=空仓
    entry_price = math.nan
    stop_loss_price = math.nan
    take_profit_price = math.nan
    pending_order = None  # {"direction": "...", "offset": "...", "volume": int}
    next_trade_day: Optional[DateClass] = None  # 下一交易日（date）

    try:
        while True:
            # 使用新的wait_update校验机制
            if not wait_update_validator.validate():
                # 检查回测是否已经结束（通过适配器访问backtest属性）
                if data_accessor.backtest and data_accessor.backtest.status == "FINISHED":
                    print("回测结束，退出循环")
                    break
                continue

            # === 账户变化落表 ===
            if data_accessor.is_changing(account, "balance") or data_accessor.is_changing(account, "available"):
                context.save_account(account)

            # === 持仓变化落表 ===
            if data_accessor.is_changing(position, "pos") or data_accessor.is_changing(position, "position_profit"):
                context.save_position(position)

            # === 用1分钟K触发“执行挂起计划” ===
            if pending_order and data_accessor.is_changing(m1.iloc[-1], "datetime"):
                m1_ts = int(m1.datetime.iloc[-1])
                m1_day = pd.to_datetime(m1_ts).date()
                if (next_trade_day is None) or (m1_day >= next_trade_day):
                    open_px = float(m1.open.iloc[-1])  # 这一分钟的开盘价
                    context.place_limit(
                        SYMBOL,
                        direction=pending_order["direction"],
                        offset=pending_order["offset"],
                        price=open_px,
                        volume=pending_order["volume"],
                    )
                    pending_order = None
                    next_trade_day = None

            # === 实时校验：检查最近订单与预期信号 ===
            if context.check_orders_changed():
                recent_orders = context.get_recent_orders()
                validator.validate_orders(recent_orders)

            # === 日线更新：计算信号 -> 仅生成计划，不立刻下单 ===
            if data_accessor.is_changing(d1.iloc[-1], "datetime"):
                new_dt = int(d1.datetime.iloc[-1])
                bar_just_begun = (last_bar_dt is not None and new_dt != last_bar_dt)
                last_bar_dt = new_dt

                # 指标准备充足？
                need = max(LONG_TERM_MA, ATR_PERIOD) + 10
                if len(d1) < need:
                    continue

                # 计算均线指标
                d1["short_ma"] = d1.close.rolling(window=SHORT_TERM_MA).mean()
                d1["long_ma"] = d1.close.rolling(window=LONG_TERM_MA).mean()
                atr_df = ATR(d1, ATR_PERIOD)

                # 获取指标值
                px_close = float(d1.close.iloc[-1])
                short_ma_curr = float(d1.short_ma.iloc[-1])
                short_ma_prev = float(d1.short_ma.iloc[-2])
                long_ma_curr = float(d1.long_ma.iloc[-1])
                long_ma_prev = float(d1.long_ma.iloc[-2])
                atr = float(atr_df.atr.iloc[-1])

                # 计算交叉信号
                golden_cross = short_ma_prev <= long_ma_prev and short_ma_curr > long_ma_curr  # 金叉
                death_cross = short_ma_prev >= long_ma_prev and short_ma_curr < long_ma_curr  # 死叉

                # 这根日线的“下一自然日”作为计划生效交易日
                this_bar_ts = int(d1.datetime.iloc[-1])
                this_day = pd.to_datetime(this_bar_ts)
                plan_day = (this_day + pd.Timedelta(days=1)).date()

                # 生成交易计划
                if current_direction == 0:
                    if golden_cross:
                        # 金叉信号，开多
                        pending_order = {"direction": "BUY", "offset": "OPEN", "volume": POSITION_SIZE}
                        next_trade_day = plan_day
                        current_direction = 1
                        entry_price = px_close
                        # 计算止损止盈
                        stop_loss_price = entry_price - STOP_LOSS_MULTIPLIER * atr
                        take_profit_price = entry_price + TAKE_PROFIT_MULTIPLIER * atr
                        # 记录预期信号用于校验
                        validator.record_expected_signal("BUY", "OPEN", POSITION_SIZE)
                        print(f"金叉信号：计划买入 {SYMBOL} {POSITION_SIZE}手")
                    elif death_cross:
                        # 死叉信号，开空
                        pending_order = {"direction": "SELL", "offset": "OPEN", "volume": POSITION_SIZE}
                        next_trade_day = plan_day
                        current_direction = -1
                        entry_price = px_close
                        # 计算止损止盈
                        stop_loss_price = entry_price + STOP_LOSS_MULTIPLIER * atr
                        take_profit_price = entry_price - TAKE_PROFIT_MULTIPLIER * atr
                        # 记录预期信号用于校验
                        validator.record_expected_signal("SELL", "OPEN", POSITION_SIZE)
                        print(f"死叉信号：计划卖出 {SYMBOL} {POSITION_SIZE}手")
                elif current_direction == 1:
                    # 多头持仓，检查止盈止损或死叉平仓
                    if (px_close <= stop_loss_price) or (px_close >= take_profit_price) or death_cross:
                        pending_order = {"direction": "SELL", "offset": "CLOSE", "volume": POSITION_SIZE}
                        next_trade_day = plan_day
                        current_direction = 0
                        # 记录预期信号用于校验
                        validator.record_expected_signal("SELL", "CLOSE", POSITION_SIZE)
                        print(f"多头平仓信号：计划卖出 {SYMBOL} {POSITION_SIZE}手")
                elif current_direction == -1:
                    # 空头持仓，检查止盈止损或金叉平仓
                    if (px_close >= stop_loss_price) or (px_close <= take_profit_price) or golden_cross:
                        pending_order = {"direction": "BUY", "offset": "CLOSE", "volume": POSITION_SIZE}
                        next_trade_day = plan_day
                        current_direction = 0
                        # 记录预期信号用于校验
                        validator.record_expected_signal("BUY", "CLOSE", POSITION_SIZE)
                        print(f"空头平仓信号：计划买入 {SYMBOL} {POSITION_SIZE}手")

                # 每根日线bar末，同步一次订单/成交到CSV
                context.snapshot_orders()

    except BacktestFinished:
        print("回测结束；数据已保存至 ./data/*.csv")
        # 最后校验一次未匹配的信号
        recent_orders = context.get_recent_orders()
        validator.validate_orders(recent_orders)
    except Exception as e:
        print(f"策略运行异常：{e}")
    finally:
        # 确保API被关闭
        data_accessor.close()
        print("API已关闭，程序正常退出")

if __name__ == "__main__":
    run_strategy()