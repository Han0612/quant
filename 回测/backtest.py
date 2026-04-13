"""
====================================================================
  Quant Master V7.2 - 回测引擎 (Vectorized Backtest Engine)
====================================================================
  策略: 强势股首阴回踩 (Strong Stock First Pullback)
  入场: 信号日收盘价买入
  出场: 分层止盈 (T1+6%, T2+8%) / -5%止损 / 第2天收盘强制清仓

  参数来源:
    退出参数 — 退出优化器最优解 (5年数据)
    策略参数 — 策略优化器最优解 (5年数据, 坐标下降+WF验证)

  架构: 全量预下载 + pandas向量化信号生成
====================================================================
"""

import tushare as ts
import pandas as pd
import numpy as np
import datetime
import time
import os
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# ╔══════════════════════════════════════════════════════════════╗
# ║                     回测配置区                               ║
# ╚══════════════════════════════════════════════════════════════╝

# --- 回测区间 ---
BACKTEST_START = '20100220'
BACKTEST_END   = '20200220'

# --- 退出参数 (优化器最优解) ---
TP_DAY1       = 0.06           # T+1 止盈: +6%
TP_DAY2       = 0.08           # T+2 止盈: +8%
TP_DAY3       = 0.08           # T+3 止盈: +8% (备用, 当前max_hold=2不会用到)
STOP_LOSS_PCT = -0.05          # 止损: -5%
MAX_HOLD_DAYS = 2              # 最大持有2天, 第2天尾盘强制清仓

# --- 交易成本 ---
COMMISSION_RATE = 0.00025
STAMP_TAX_RATE  = 0.0005
SLIPPAGE_RATE   = 0.0005
TOTAL_COST = COMMISSION_RATE * 2 + STAMP_TAX_RATE + SLIPPAGE_RATE * 2

# --- 策略参数 (优化器最优解) ---
# 爆量参数
EXPLOSION_LOOKBACK_DAYS = 16   # 回溯16个交易日寻找爆量日
EXPLOSION_PCT_MIN  = 7.5       # 爆量日最低涨幅7.5%
EXPLOSION_PCT_MAX  = 16.0      # 爆量日最高涨幅16% (排除过度投机)
EXPLOSION_VOL_MULT = 1.4       # 成交量 > 5日均量 × 1.4

# 均线排列
BULL_DAYS_WINDOW     = 8       # 检查最近8个交易日
BULL_DAYS_THRESHOLD  = 8       # 要求8/8天 MA10 > MA20
MA20_MA30_TOLERANCE  = 1.008   # MA20 > MA30 × 1.008 (要求明确分离)
MA10_SLOPE_TOLERANCE = 0.993   # MA10 >= 昨日MA10 × 0.993 (允许微降)

# 距离/位置
MA30_SUPPORT_TOLERANCE = 0.98  # 股价 >= MA30 × 0.98
MAX_DIST_MA10      = 0.025     # 股价 < MA10 × (1 + 2.5%)
MAX_DIST_MA10_LOW  = 0.02      # 股价 > MA10 × (1 + 2.0%)  ⚠️ 注意: 极窄0.5%带
MAX_DIST_MA10_MA20 = 0.025     # MA10-MA20 乖离 < 2.5%
MAX_20_DAYS_RISE   = 0.35      # 20日涨幅 < 35%

# --- 下载配置 ---
DOWNLOAD_SLEEP = 0.12
WARMUP_CALENDAR_DAYS = 150
BUFFER_CALENDAR_DAYS = 20

# 止盈查找表
TP_BY_DAY = {1: TP_DAY1, 2: TP_DAY2, 3: TP_DAY3}

# ╔══════════════════════════════════════════════════════════════╗
# ║                     Tushare 初始化                           ║
# ╚══════════════════════════════════════════════════════════════╝
print("=" * 60)
print("  🚀 Quant Master V7.2 — Backtest Engine (Optimized)")
print("=" * 60)

pro = ts.pro_api('init_placeholder')
pro._DataApi__token    = '4502048184048033923'
pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'
print("✅ 数据通道已激活\n")


# ╔══════════════════════════════════════════════════════════════╗
# ║                     数据管理器                               ║
# ╚══════════════════════════════════════════════════════════════╝
class DataManager:

    def __init__(self):
        self.df = None
        self.trade_dates = []

    def download_and_prepare(self, bt_start, bt_end):

        # Step 1: 交易日历
        print("📅 [1/5] 下载交易日历...")
        warmup_start = (datetime.datetime.strptime(bt_start, '%Y%m%d')
                        - datetime.timedelta(days=WARMUP_CALENDAR_DAYS)).strftime('%Y%m%d')
        buffer_end = (datetime.datetime.strptime(bt_end, '%Y%m%d')
                      + datetime.timedelta(days=BUFFER_CALENDAR_DAYS)).strftime('%Y%m%d')

        cal = pro.trade_cal(exchange='SSE', start_date=warmup_start, end_date=buffer_end)
        open_dates = sorted(cal[cal['is_open'] == 1]['cal_date'].tolist())

        self.trade_dates = [d for d in open_dates if bt_start <= d <= bt_end]
        print(f"   回测区间: {len(self.trade_dates)} 个交易日")
        print(f"   含预热共: {len(open_dates)} 个交易日需下载")

        # Step 2: 批量下载日线
        print(f"\n📊 [2/5] 下载全市场日线 ({len(open_dates)} 天)...")
        all_dfs = []
        fail_count = 0

        for date in tqdm(open_dates, desc="日线下载", ncols=80):
            success = False
            for attempt in range(3):
                try:
                    df_day = pro.daily(trade_date=date)
                    if df_day is not None and not df_day.empty:
                        all_dfs.append(df_day)
                        success = True
                        break
                except:
                    time.sleep(0.5)
            if not success:
                fail_count += 1
            time.sleep(DOWNLOAD_SLEEP)

        if fail_count > 0:
            print(f"   ⚠️ {fail_count} 个交易日下载失败")

        df = pd.concat(all_dfs, ignore_index=True)
        print(f"   原始记录: {len(df):,} 条")

        # Step 3: 过滤主板
        print(f"\n🔍 [3/5] 过滤主板...")
        df = df[df['ts_code'].str[:2].isin(['60', '00'])].copy()
        n_stocks = df['ts_code'].nunique()
        print(f"   主板记录: {len(df):,} 条 ({n_stocks} 只股票)")

        # Step 4: 复权 + 特征
        print(f"\n⚙️  [4/5] 复权 & 特征计算...")
        df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
        df = self._compute_adjusted_prices(df)
        df = self._compute_features(df)

        # Step 5: 预算未来价格
        print(f"\n📈 [5/5] 预算退出价格 (T+1 ~ T+{MAX_HOLD_DAYS} 完整OHLC)...")
        df = self._compute_future_prices(df)

        self.df = df
        bt_rows = len(df[df['trade_date'].isin(self.trade_dates)])
        print(f"\n✅ 数据就绪! 回测区间可扫描: {bt_rows:,} 条")
        return df

    def _compute_adjusted_prices(self, df):
        def _adj_group(g):
            g = g.sort_values('trade_date').copy()
            pct = g['pct_chg'].fillna(0).values
            cum_ret = np.cumprod(1 + pct / 100)
            last_close = g['close'].values[-1]
            last_cum = cum_ret[-1]
            if last_cum == 0 or np.isnan(last_cum):
                g['adj_close'] = g['close']
                g['adj_open']  = g['open']
                g['adj_high']  = g['high']
                g['adj_low']   = g['low']
                return g
            g['adj_close'] = last_close * cum_ret / last_cum
            raw_close = g['close'].replace(0, np.nan)
            factor = (g['adj_close'] / raw_close).fillna(1)
            g['adj_open'] = g['open'] * factor
            g['adj_high'] = g['high'] * factor
            g['adj_low']  = g['low']  * factor
            return g
        tqdm.pandas(desc="复权计算", ncols=80)
        df = df.groupby('ts_code', group_keys=False).progress_apply(_adj_group)
        return df

    def _compute_features(self, df):
        g = df.groupby('ts_code')

        # 均线
        df['MA10'] = g['adj_close'].transform(lambda x: x.rolling(10, min_periods=10).mean())
        df['MA20'] = g['adj_close'].transform(lambda x: x.rolling(20, min_periods=20).mean())
        df['MA30'] = g['adj_close'].transform(lambda x: x.rolling(30, min_periods=30).mean())
        df['Vol_MA5'] = g['vol'].transform(lambda x: x.rolling(5, min_periods=5).mean())

        # 昨日数据
        df['yest_close']  = g['adj_close'].shift(1)
        df['yest_volume'] = g['vol'].shift(1)
        df['yest_MA10']   = g['MA10'].shift(1)
        df['yest_MA20']   = g['MA20'].shift(1)

        # 20日前收盘价
        df['close_20d_ago'] = g['adj_close'].shift(19)

        # 爆量日标记
        df['is_explosion_day'] = (
            (
                (df['pct_chg'] >= EXPLOSION_PCT_MIN) &
                (df['pct_chg'] <= EXPLOSION_PCT_MAX) &
                (df['vol'] > df['Vol_MA5'] * EXPLOSION_VOL_MULT)
            ) | (
                (df['close'] == df['open']) & (df['pct_chg'] > 9.5)
            )
        ).astype(float)

        # 过去N日内有爆量 (shift(1): 不含当天)
        df['has_explosion'] = g['is_explosion_day'].transform(
            lambda x: x.shift(1).rolling(EXPLOSION_LOOKBACK_DAYS, min_periods=1).max()
        )

        # 多头排列天数 (8日窗口)
        df['_bf'] = (df['MA10'] > df['MA20']).astype(float)
        df['bull_days'] = g['_bf'].transform(
            lambda x: x.rolling(BULL_DAYS_WINDOW, min_periods=BULL_DAYS_WINDOW).sum()
        )
        df.drop(columns=['_bf'], inplace=True)

        # 金叉判定
        df['is_golden_cross'] = (
            (df['MA10'] > df['MA20']) &
            (df['yest_MA10'] <= df['yest_MA20']) &
            (df['MA10'] > df['yest_MA10'])
        )

        print(f"   特征计算完成 (列数: {len(df.columns)})")
        return df

    def _compute_future_prices(self, df):
        g = df.groupby('ts_code')
        for day in range(1, MAX_HOLD_DAYS + 1):
            df[f'next{day}_open']  = g['adj_open'].shift(-day)
            df[f'next{day}_high']  = g['adj_high'].shift(-day)
            df[f'next{day}_low']   = g['adj_low'].shift(-day)
            df[f'next{day}_close'] = g['adj_close'].shift(-day)
            df[f'next{day}_date']  = g['trade_date'].shift(-day)
        return df


# ╔══════════════════════════════════════════════════════════════╗
# ║                     退出模拟器                               ║
# ╚══════════════════════════════════════════════════════════════╝

def simulate_exit(row, entry_price):
    """
    分层止盈退出模拟 (单笔交易)

    ┌──────────┬──────────┬──────────┬──────────────┐
    │ 持有天数 │  止盈线  │  止损线  │    到期处理    │
    ├──────────┼──────────┼──────────┼──────────────┤
    │   T+1    │   +6%    │   -5%    │      —       │
    │   T+2    │   +8%    │   -5%    │ 收盘强制清仓  │
    └──────────┴──────────┴──────────┴──────────────┘

    日内优先级:
      1. 开盘跳空穿越 → 以开盘价成交 (gap)
      2. 同日双触发   → 止损优先 (保守)
      3. 单边触发     → 以触发价成交
      4. 最后一天未触发 → 收盘价强平
    """
    sl_price = entry_price * (1 + STOP_LOSS_PCT)

    for day in range(1, MAX_HOLD_DAYS + 1):
        d_open  = row.get(f'next{day}_open',  np.nan)
        d_high  = row.get(f'next{day}_high',  np.nan)
        d_low   = row.get(f'next{day}_low',   np.nan)
        d_close = row.get(f'next{day}_close', np.nan)

        if pd.isna(d_open) or pd.isna(d_high) or pd.isna(d_low) or pd.isna(d_close):
            continue

        tp_pct = TP_BY_DAY.get(day, TP_DAY3)
        tp_price = entry_price * (1 + tp_pct)

        # ─── 开盘跳空 ───
        if d_open >= tp_price:
            return d_open, day, f'TP_GAP_D{day}'
        if d_open <= sl_price:
            return d_open, day, f'SL_GAP_D{day}'

        # ─── 盘中触发 ───
        hit_tp = (d_high >= tp_price)
        hit_sl = (d_low  <= sl_price)

        if hit_tp and hit_sl:
            return sl_price, day, f'SL_D{day}'
        if hit_tp:
            return tp_price, day, f'TP_D{day}'
        if hit_sl:
            return sl_price, day, f'SL_D{day}'

        # ─── 最后一天收盘强平 ───
        if day == MAX_HOLD_DAYS:
            return d_close, day, 'TIMEOUT'

    return np.nan, np.nan, 'NO_DATA'


# ╔══════════════════════════════════════════════════════════════╗
# ║                     回测引擎                                 ║
# ╚══════════════════════════════════════════════════════════════╝
class BacktestEngine:

    def run(self, df, trade_dates):

        print("\n" + "=" * 60)
        print("  🔎 阶段一: 信号生成")
        print("=" * 60)
        signals = self._generate_signals(df, trade_dates)

        if signals.empty:
            print("❌ 回测期间无任何信号。")
            return None, None

        print(f"\n  ✅ 总信号数: {len(signals)}")
        print(f"  📅 日均信号: {len(signals)/len(trade_dates):.1f} 只/天")

        print("\n" + "=" * 60)
        print(f"  💹 阶段二: 交易模拟 (分层止盈)")
        print(f"      T+1: +{TP_DAY1*100}%  T+2: +{TP_DAY2*100}%  止损: {STOP_LOSS_PCT*100}%  最长: {MAX_HOLD_DAYS}天")
        print("=" * 60)
        trades = self._simulate_trades(signals)

        if trades is None or trades.empty:
            print("❌ 无有效交易")
            return signals, None

        print(f"  ✅ 有效交易: {len(trades)} 笔")

        print("\n" + "=" * 60)
        print("  📊 阶段三: 绩效分析")
        print("=" * 60)
        self._print_report(trades, trade_dates)

        return signals, trades

    def _generate_signals(self, df, trade_dates):
        in_range = df['trade_date'].isin(trade_dates)

        # 规则1: 16日内有爆量上涨
        f1 = df['has_explosion'] == 1

        # 规则2: 多头排列 (8/8天 MA10>MA20) + MA20>MA30×1.008
        f2_stable = df['bull_days'] >= BULL_DAYS_THRESHOLD
        f2_cross  = df['is_golden_cross']
        f2_2030   = df['MA20'] > df['MA30'] * MA20_MA30_TOLERANCE
        f2 = (f2_stable | f2_cross) & f2_2030

        # 规则3: MA10上斜 (允许微降0.7%)
        f3 = df['MA10'] >= df['yest_MA10'] * MA10_SLOPE_TOLERANCE

        # 规则4: 站稳MA30 (允许偏离2%)
        f4 = df['adj_close'] >= df['MA30'] * MA30_SUPPORT_TOLERANCE

        # 规则5: MA10接近MA20 (乖离率 < 2.5%)
        ma_spread = (df['MA10'] - df['MA20']) / df['MA20']
        f5 = (ma_spread < MAX_DIST_MA10_MA20) & (df['MA20'] > 0)

        # 规则6: 阴线回调 + 缩量 (联合)
        is_pullback = (df['adj_close'] < df['yest_close']) | (df['adj_close'] < df['adj_open'])
        is_shrink   = df['vol'] < df['yest_volume']
        f6 = is_pullback & is_shrink

        # 规则7: 非高位 (20日涨幅 < 35%)
        rise_20d = (df['adj_close'] - df['close_20d_ago']) / df['close_20d_ago']
        f7 = rise_20d < MAX_20_DAYS_RISE

        # 规则8: 击球区 (股价在MA10上方 2.0%~2.5%)
        dist_ma10 = (df['adj_close'] - df['MA10']) / df['MA10']
        f8 = (dist_ma10 > MAX_DIST_MA10_LOW) & (dist_ma10 < MAX_DIST_MA10)

        # 全部通过
        all_pass = in_range & f1 & f2 & f3 & f4 & f5 & f6 & f7 & f8
        signals = df[all_pass].copy()

        # 淘汰漏斗
        remaining = in_range.copy()
        filters_list = [
            (f'爆量基因({EXPLOSION_LOOKBACK_DAYS}日)', f1),
            (f'多头排列({BULL_DAYS_THRESHOLD}/{BULL_DAYS_WINDOW})', f2),
            ('MA10上斜',        f3),
            ('MA30之上',        f4),
            (f'乖离<{MAX_DIST_MA10_MA20*100}%', f5),
            ('阴线+缩量',       f6),
            (f'20日涨幅<{int(MAX_20_DAYS_RISE*100)}%', f7),
            (f'MA10击球区({MAX_DIST_MA10_LOW*100}-{MAX_DIST_MA10*100}%)', f8),
        ]
        print(f"\n  📊 淘汰漏斗 (可扫描: {remaining.sum():,} 条)")
        print("  " + "-" * 50)
        for name, f in filters_list:
            before = remaining.sum()
            remaining = remaining & f
            after = remaining.sum()
            dropped = before - after
            print(f"  ❌ {name:<25} 淘汰 {dropped:>7,}  (剩余 {after:>7,})")
        print("  " + "-" * 50)
        print(f"  ✅ 最终入围信号: {remaining.sum():,}")
        return signals

    def _simulate_trades(self, signals):
        trades = signals.copy()
        trades['entry_price'] = trades['adj_close']

        exit_prices, hold_days_list, exit_types = [], [], []
        for _, row in tqdm(trades.iterrows(), total=len(trades), desc="交易模拟", ncols=80):
            ep, hd, et = simulate_exit(row, row['entry_price'])
            exit_prices.append(ep)
            hold_days_list.append(hd)
            exit_types.append(et)

        trades['exit_price'] = exit_prices
        trades['hold_days']  = hold_days_list
        trades['exit_type']  = exit_types

        trades = trades.dropna(subset=['exit_price', 'entry_price'])
        trades = trades[trades['entry_price'] > 0].copy()

        trades['gross_return'] = trades['exit_price'] / trades['entry_price'] - 1
        trades['net_return']   = trades['gross_return'] - TOTAL_COST
        trades['is_win']       = trades['net_return'] > 0

        def _exit_category(et):
            if 'TP' in str(et): return 'TP(止盈)'
            if 'SL' in str(et): return 'SL(止损)'
            if et == 'TIMEOUT':  return 'TIMEOUT(到期)'
            return str(et)

        trades['exit_cat'] = trades['exit_type'].apply(_exit_category)

        # 退出明细
        print(f"\n  📊 退出类型分布 (明细):")
        for et in sorted(trades['exit_type'].unique()):
            sub = trades[trades['exit_type'] == et]
            cnt = len(sub)
            avg_r = sub['net_return'].mean() * 100
            wr = sub['is_win'].mean() * 100
            labels = {
                'TP_D1': f'✅ T+1止盈(+{TP_DAY1*100}%)',
                'TP_D2': f'✅ T+2止盈(+{TP_DAY2*100}%)',
                'TP_GAP_D1': f'🚀 T+1跳空止盈',
                'TP_GAP_D2': f'🚀 T+2跳空止盈',
                'SL_D1': f'❌ T+1止损({STOP_LOSS_PCT*100}%)',
                'SL_D2': f'❌ T+2止损({STOP_LOSS_PCT*100}%)',
                'SL_GAP_D1': f'💀 T+1跳空止损',
                'SL_GAP_D2': f'💀 T+2跳空止损',
                'TIMEOUT':   f'⏰ 第{MAX_HOLD_DAYS}天收盘清仓',
            }
            label = labels.get(et, et)
            print(f"     {label:<22} {cnt:>5} 笔  胜率:{wr:>5.1f}%  均收益:{avg_r:+.2f}%")

        # 大类汇总
        print(f"\n  📊 退出大类汇总:")
        for cat in ['TP(止盈)', 'SL(止损)', 'TIMEOUT(到期)']:
            sub = trades[trades['exit_cat'] == cat]
            if len(sub) == 0: continue
            cnt = len(sub)
            pct = cnt / len(trades) * 100
            avg_r = sub['net_return'].mean() * 100
            wr = sub['is_win'].mean() * 100
            print(f"     {cat:<16} {cnt:>5} 笔 ({pct:>5.1f}%)  胜率:{wr:>5.1f}%  均收益:{avg_r:+.2f}%")

        return trades

    def _print_report(self, trades, trade_dates):
        n = len(trades)
        if n == 0:
            print("  无交易数据。"); return

        wins   = int(trades['is_win'].sum())
        losses = n - wins
        ret = trades['net_return']
        avg_ret  = ret.mean() * 100
        avg_win  = ret[trades['is_win']].mean() * 100 if wins > 0 else 0
        avg_loss = ret[~trades['is_win']].mean() * 100 if losses > 0 else 0
        win_rate = wins / n * 100
        profit_factor = abs(avg_win * wins / (avg_loss * losses)) if (losses > 0 and avg_loss != 0) else float('inf')
        expectancy = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)
        max_win  = ret.max() * 100
        max_loss = ret.min() * 100
        avg_hold = trades['hold_days'].mean()

        t_sorted = trades.sort_values('trade_date').copy()
        t_sorted['cum_nav'] = (1 + t_sorted['net_return']).cumprod()
        peak = t_sorted['cum_nav'].cummax()
        dd = (t_sorted['cum_nav'] - peak) / peak
        max_dd = dd.min() * 100
        total_ret = (t_sorted['cum_nav'].iloc[-1] - 1) * 100

        days_in_bt = len(trade_dates)
        years = days_in_bt / 250
        annual_ret = ((1 + total_ret / 100) ** (1 / max(years, 0.01)) - 1) * 100

        if n > 1 and ret.std() > 0:
            trades_per_year = n / max(years, 0.01)
            sharpe = (ret.mean() * trades_per_year - 0.02) / (ret.std() * np.sqrt(trades_per_year))
        else:
            sharpe = 0

        t_sorted['month'] = t_sorted['trade_date'].str[:6]
        monthly = t_sorted.groupby('month').agg(
            trades_count=('net_return', 'count'),
            avg_ret=('net_return', 'mean'),
            win_rate=('is_win', 'mean'),
            total_ret=('net_return', 'sum')
        )

        hold_dist = trades['hold_days'].value_counts().sort_index()

        print(f"""
╔══════════════════════════════════════════════════════════════════╗
║              BACKTEST PERFORMANCE REPORT (Optimized)            ║
╠══════════════════════════════════════════════════════════════════╣
║  回测区间  : {trade_dates[0]} ~ {trade_dates[-1]}
║  交易天数  : {days_in_bt} 天 ({years:.1f} 年)
║  退出规则  : T1+{TP_DAY1*100}% / T2+{TP_DAY2*100}% 止盈
║              {STOP_LOSS_PCT*100}% 止损 / {MAX_HOLD_DAYS}天强平
║  交易成本  : {TOTAL_COST*100:.2f}%
╠══════════════════════════════════════════════════════════════════╣
║  🔧 策略参数:
║     爆量: {EXPLOSION_LOOKBACK_DAYS}日 涨幅{EXPLOSION_PCT_MIN}-{EXPLOSION_PCT_MAX}% 量比>{EXPLOSION_VOL_MULT}
║     多头: {BULL_DAYS_THRESHOLD}/{BULL_DAYS_WINDOW}天 MA20>MA30×{MA20_MA30_TOLERANCE}
║     击球: MA10+{MAX_DIST_MA10_LOW*100}%~+{MAX_DIST_MA10*100}%  乖离<{MAX_DIST_MA10_MA20*100}%
║     限制: 20日涨幅<{MAX_20_DAYS_RISE*100}%  MA30支撑>{MA30_SUPPORT_TOLERANCE}
╠══════════════════════════════════════════════════════════════════╣
║  交易笔数  : {n} 笔 (日均 {n/days_in_bt:.1f})
║  ✅ 胜率    : {win_rate:.1f}% ({wins}胜 / {losses}负)
║  📈 平均收益 : {avg_ret:+.2f}%
║  📈 平均盈利 : {avg_win:+.2f}%
║  📉 平均亏损 : {avg_loss:+.2f}%
║  ⚖️  盈亏比  : {profit_factor:.2f}
║  🎯 单笔期望 : {expectancy:+.2f}%
║  ⏱️  平均持有 : {avg_hold:.1f} 天
╠══════════════════════════════════════════════════════════════════╣
║  💰 累计收益 : {total_ret:+.1f}% (逐笔复利)
║  📊 年化收益 : {annual_ret:+.1f}%
║  📉 最大回撤 : {max_dd:.1f}%
║  📊 Sharpe   : {sharpe:.2f}
║  🏆 最大单笔盈: {max_win:+.1f}%
║  💀 最大单笔亏: {max_loss:+.1f}%
╚══════════════════════════════════════════════════════════════════╝""")

        # 持有天数分布
        print(f"\n  ⏱️  持有天数分布:")
        for days, cnt in hold_dist.items():
            pct = cnt / n * 100
            bar = "█" * int(pct / 2)
            avg_r = trades[trades['hold_days'] == days]['net_return'].mean() * 100
            wr = trades[trades['hold_days'] == days]['is_win'].mean() * 100
            tp_line = TP_BY_DAY.get(int(days), TP_DAY3) * 100
            print(f"     T+{int(days)} (止盈+{tp_line}%): {cnt:>5} 笔 ({pct:>5.1f}%) "
                  f"{bar:<25} 胜率:{wr:.0f}% 均收益:{avg_r:+.2f}%")

        # 月度明细
        print(f"\n  📅 月度明细:")
        print(f"  {'月份':<8} {'笔数':>5} {'胜率':>7} {'平均收益':>9} {'月累计':>9}")
        print("  " + "-" * 42)
        for month, mrow in monthly.iterrows():
            wr = mrow['win_rate'] * 100
            ar = mrow['avg_ret'] * 100
            tr = mrow['total_ret'] * 100
            cnt = int(mrow['trades_count'])
            flag = "🟢" if tr > 0 else "🔴"
            print(f"  {flag} {month:<6} {cnt:>5} {wr:>6.1f}% {ar:>+8.2f}% {tr:>+8.1f}%")
        print("  " + "-" * 42)

        # 收益分布
        pcts_q = [5, 10, 25, 50, 75, 90, 95]
        percentiles = np.percentile(ret * 100, pcts_q)
        print(f"\n  📊 收益分布 (百分位):")
        labels = " | ".join([f"P{p}={v:+.1f}%" for p, v in zip(pcts_q, percentiles)])
        print(f"  {labels}")

        # 连续亏损
        streak = 0; max_streak = 0
        for w in t_sorted['is_win']:
            if not w: streak += 1; max_streak = max(max_streak, streak)
            else: streak = 0
        print(f"\n  💀 最大连续亏损笔数: {max_streak}")

        # TOP / BOTTOM
        print(f"\n  🏆 最佳5笔:")
        top5 = trades.nlargest(5, 'net_return')[
            ['trade_date', 'ts_code', 'net_return', 'hold_days', 'exit_type']]
        for _, r in top5.iterrows():
            print(f"     {r['trade_date']} {r['ts_code']} {r['net_return']*100:+.1f}% "
                  f"(T+{int(r['hold_days'])} {r['exit_type']})")
        print(f"\n  💀 最差5笔:")
        bot5 = trades.nsmallest(5, 'net_return')[
            ['trade_date', 'ts_code', 'net_return', 'hold_days', 'exit_type']]
        for _, r in bot5.iterrows():
            print(f"     {r['trade_date']} {r['ts_code']} {r['net_return']*100:+.1f}% "
                  f"(T+{int(r['hold_days'])} {r['exit_type']})")

        return t_sorted


# ╔══════════════════════════════════════════════════════════════╗
# ║                        主程序                               ║
# ╚══════════════════════════════════════════════════════════════╝
if __name__ == '__main__':

    print(f"\n  回测区间  : {BACKTEST_START} ~ {BACKTEST_END}")
    print(f"  退出规则  : T+1止盈+{TP_DAY1*100}% / T+2止盈+{TP_DAY2*100}%")
    print(f"              止损{STOP_LOSS_PCT*100}% / 第{MAX_HOLD_DAYS}天收盘强平")
    print(f"  交易成本  : {TOTAL_COST*100:.2f}%")
    print(f"  策略参数  : 爆量{EXPLOSION_LOOKBACK_DAYS}日 涨幅{EXPLOSION_PCT_MIN}-{EXPLOSION_PCT_MAX}%"
          f" 量比>{EXPLOSION_VOL_MULT}")
    print(f"              多头{BULL_DAYS_THRESHOLD}/{BULL_DAYS_WINDOW} 乖离<{MAX_DIST_MA10_MA20*100}%"
          f" 击球区MA10+{MAX_DIST_MA10_LOW*100}-{MAX_DIST_MA10*100}%"
          f" 涨幅<{MAX_20_DAYS_RISE*100}%")
    print()

    dm = DataManager()
    df = dm.download_and_prepare(BACKTEST_START, BACKTEST_END)

    engine = BacktestEngine()
    signals, trades = engine.run(df, dm.trade_dates)

    if trades is not None and not trades.empty:
        out_cols = ['trade_date', 'ts_code', 'entry_price', 'exit_price',
                    'hold_days', 'exit_type', 'gross_return', 'net_return', 'is_win']
        fname = f"BT_trades_{BACKTEST_START}_{BACKTEST_END}.csv"
        trades[out_cols].to_csv(fname, index=False, encoding='utf_8_sig')
        print(f"\n  ✅ 交易明细 → {fname}")

        sig_cols = ['trade_date', 'ts_code', 'adj_close', 'pct_chg',
                    'vol', 'MA10', 'MA20', 'MA30']
        sig_cols = [c for c in sig_cols if c in signals.columns]
        sfname = f"BT_signals_{BACKTEST_START}_{BACKTEST_END}.csv"
        signals[sig_cols].to_csv(sfname, index=False, encoding='utf_8_sig')
        print(f"  ✅ 信号明细 → {sfname}")

        nav = trades.sort_values('trade_date')[
            ['trade_date', 'ts_code', 'net_return']].copy()
        nav['cum_nav'] = (1 + nav['net_return']).cumprod()
        nav_fname = f"BT_nav_{BACKTEST_START}_{BACKTEST_END}.csv"
        nav.to_csv(nav_fname, index=False, encoding='utf_8_sig')
        print(f"  ✅ 净值曲线 → {nav_fname}")

    print("\n  🏁 回测完成。")