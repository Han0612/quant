"""
╔══════════════════════════════════════════════════════════════════╗
║  Quant Master V7.2 Turbo + Diagnostic (ST_CLIENT 专享版)         ║
║  10000积分专用: 按日期批量下载 + 向量化筛选                      ║
╚══════════════════════════════════════════════════════════════════╝
python Claude_Tail_Buy_Diag.py --diag 002429,000001,600519

支持输入格式：`002429` / `002429.SZ` / `600519` / `600519.SH`，会自动补全后缀。
"""

import tushare as ts # 仅保留用于无Token限制的 ts.get_realtime_quotes
import pandas as pd
import numpy as np
import datetime
import time
import os
import random
import sys
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings

# ================= 0. 系统配置区 =================
warnings.filterwarnings('ignore')
pd.set_option('mode.chained_assignment', None)

MAX_WORKERS = 10
stats_lock = threading.Lock()

filter_stats = {
    "0_data_missing": 0,
    "1_trend_error": 0,
    "2_multi_head_error": 0,
    "3_ma_dist_error": 0,
    "4_candle_vol_error": 0,
    "5_vol_error": 0,
    "6_price_pos_error": 0,
    "7_high_pos_error": 0,
    "8_explode_error": 0,
    "99_unknown_error": 0,
}

# ================= 🔑 自定义 API 客户端初始化 =================
try:
    from ST_CLIENT import StockToday
except ImportError:
    print("❌ 未找到 ST_CLIENT.py，请确保该文件与当前脚本在同一目录下。")
    exit()

print("🚀 启动 Quant Master V7.2 Turbo + Diagnostic (ST_CLIENT 版)...")

try:
    # 替换官方初始化为自定义网关
    pro = StockToday()
    print("✅ ST_CLIENT 专用数据通道已激活。")
except Exception as e:
    print(f"❌ 初始化失败: {e}")
    exit()

# ╔══════════════════════════════════════════════════════════════╗
# ║              策略参数 (优化器最优解)                         ║
# ║  来源: 5年数据坐标下降 + Walk-Forward验证                    ║
# ║  退出: TP1=+6% TP2=+8% SL=-5% Hold=2天                       ║
# ╚══════════════════════════════════════════════════════════════╝

# --- 爆量参数 ---
EXPLOSION_LOOKBACK_DAYS = 16
EXPLOSION_PCT_MIN  = 7.5
EXPLOSION_PCT_MAX  = 16.0
EXPLOSION_VOL_MULT = 1.4

# --- 均线排列 ---
BULL_DAYS_WINDOW     = 8
BULL_DAYS_THRESHOLD  = 8
MA20_MA30_TOLERANCE  = 1.008
MA10_SLOPE_TOLERANCE = 0.993

# --- 距离/位置 ---
MA30_SUPPORT_TOLERANCE = 0.98
MAX_DIST_MA10      = 0.025
MAX_DIST_MA10_LOW  = 0.001  # 击球下限
MAX_DIST_MA10_MA20 = 0.045  # 乖离率：[0.025, 0.035, 0.045]
MAX_20_DAYS_RISE   = 0.35

# --- 风控标记 ---
AMP_THRESHOLD = 0.06
TURNOVER_RATIO_LIMIT = 1.5

# ================= 辅助函数 =================

def _safe_api_call(func, *args, retries=3, **kwargs):
    """
    带重试的安全API调用
    核心改造：自动适配 ST_CLIENT 返回的 JSON List，将其转换为 pd.DataFrame
    """
    for i in range(retries):
        try:
            result = func(*args, **kwargs)
            
            # 格式转换逻辑
            if isinstance(result, list):
                df = pd.DataFrame(result)
            elif isinstance(result, pd.DataFrame):
                df = result
            else:
                df = pd.DataFrame()

            if not df.empty:
                return df
        except Exception as e:
            if i < retries - 1:
                time.sleep(0.3 * (i + 1))
    return pd.DataFrame()


def get_intraday_volume_completion(minutes_elapsed):
    """A股日内成交量U型分布模型 (分段线性插值)"""
    CURVE = [
        (0, 0.000), (10, 0.055), (20, 0.100), (30, 0.145),
        (45, 0.195), (60, 0.245), (90, 0.325), (120, 0.420),
        (135, 0.480), (150, 0.535), (165, 0.590), (180, 0.645),
        (195, 0.715), (210, 0.790), (220, 0.850), (230, 0.920),
        (240, 1.000),
    ]
    if minutes_elapsed <= 0:
        return 0.01
    if minutes_elapsed >= 240:
        return 1.0
    for i in range(len(CURVE) - 1):
        t0, r0 = CURVE[i]
        t1, r1 = CURVE[i + 1]
        if t0 <= minutes_elapsed <= t1:
            return r0 + (minutes_elapsed - t0) / (t1 - t0) * (r1 - r0)
    return 1.0


def calc_trading_minutes_elapsed():
    """当前距开盘已过交易分钟数 (0~240)"""
    now = datetime.datetime.now()
    if now.weekday() >= 5:
        return 240
    mins = now.hour * 60 + now.minute
    if mins < 570:    return 0
    elif mins <= 690: return mins - 570
    elif mins < 780:  return 120
    elif mins <= 900: return 120 + (mins - 780)
    else:             return 240


def get_daily_basic_bulk():
    """批量拉取全市场基本面 (float_share, circ_mv)"""
    print("⚡ 拉取基本面数据...")
    for delta in range(10):
        d_str = (datetime.datetime.now() - datetime.timedelta(days=delta)).strftime('%Y%m%d')
        try:
            # 适配 ST_CLIENT
            df = _safe_api_call(pro.daily_basic, trade_date=d_str,
                                fields='ts_code,float_share,circ_mv,pe_ttm,turnover_rate')
            if not df.empty and len(df) > 2000:
                print(f"  ✅ 命中 {d_str} ({len(df)} 条)")
                return df.rename(columns={'turnover_rate': 'last_turnover'})
        except:
            time.sleep(0.5)
    print("  ⚠️ daily_basic 获取失败")
    return pd.DataFrame()


def resolve_diag_codes(raw_codes):
    """
    将用户输入的股票代码(如 002429, 000001) 转为 ts_code 格式 (002429.SZ, 000001.SZ)
    """
    resolved = []
    for code in raw_codes:
        code = code.strip()
        if '.' in code:
            resolved.append(code.upper())
        elif code.startswith('6'):
            resolved.append(f"{code}.SH")
        else:
            resolved.append(f"{code}.SZ")
    return resolved


# ================= 第一阶段: 技术面筛选 =================

class TailEndStrategy:

    LOOKBACK_CALENDAR_DAYS = 110

    def __init__(self, diag_codes=None):
        self.diag_codes = diag_codes or []

    def get_market_snapshot(self):
        """实时行情快照"""
        try:
            print("📋 拉取基础股票列表...")
            data = None
            for attempt in range(3):
                try:
                    # 适配 ST_CLIENT
                    data = _safe_api_call(pro.stock_basic, exchange='', list_status='L')
                    if data is not None and not data.empty and len(data) > 4000:
                        break
                    time.sleep(1)
                except Exception as e:
                    print(f"  ⚠️ 请求失败 (第{attempt+1}次): {e}")
                    time.sleep(1)

            if data is None or data.empty or len(data) < 4000:
                raise Exception("基础列表获取失败")

            data = data[data['symbol'].str.startswith(('60', '00'))]
            print(f"  ✅ 主板 {len(data)} 只")

            df_basics = get_daily_basic_bulk()

            print("📡 拉取实时行情 (Crawler Mode)...")
            code_list = data['symbol'].tolist()
            realtime_dfs = []
            batch_size = 100

            for i in tqdm(range(0, len(code_list), batch_size), desc="  实时行情"):
                batch = code_list[i:i + batch_size]
                for attempt in range(3):
                    try:
                        time.sleep(random.uniform(0.05, 0.15))
                        # 保持使用原生的 tushare 实时爬虫接口
                        df_batch = ts.get_realtime_quotes(batch)
                        if df_batch is not None and not df_batch.empty and 'code' in df_batch.columns:
                            realtime_dfs.append(df_batch)
                            break
                    except:
                        time.sleep(0.5)

            if not realtime_dfs:
                raise Exception("无法获取实时行情")

            df_rt = pd.concat(realtime_dfs, ignore_index=True)
            if 'code' not in df_rt.columns:
                df_rt.columns = [c.strip() for c in df_rt.columns]

            data['symbol'] = data['symbol'].astype(str).str.strip()
            df_rt['code'] = df_rt['code'].astype(str).str.strip()

            df_final = pd.merge(df_rt, data[['symbol', 'ts_code', 'industry']],
                                left_on='code', right_on='symbol', how='inner')

            if not df_basics.empty:
                df_final = pd.merge(df_final, df_basics, on='ts_code', how='left')
                df_final['float_share'] = df_final['float_share'].fillna(0)
                df_final['circ_mv'] = df_final['circ_mv'].fillna(0)
            else:
                df_final['float_share'] = 0
                df_final['circ_mv'] = 0

            df_final = df_final.rename(columns={
                'price': 'close', 'volume': 'volume_raw', 'amount': 'amount_raw'
            })
            for c in ['close', 'high', 'low', 'open', 'pre_close', 'volume_raw', 'amount_raw']:
                if c in df_final.columns:
                    df_final[c] = pd.to_numeric(df_final[c], errors='coerce').fillna(0)

            df_final['pre_close'] = df_final['pre_close'].replace(0, np.nan)
            df_final['pct_chg'] = (
                (df_final['close'] - df_final['pre_close']) / df_final['pre_close'] * 100
            ).fillna(0)
            df_final['volume'] = df_final['volume_raw'] / 100
            df_final['amount'] = df_final['amount_raw'] / 1000

            return df_final

        except Exception as e:
            print(f"❌ 行情初始化失败: {e}")
            raise

    # ─────────────────────────────────────────────────────
    # 🚀 核心加速: 按日期批量下载
    # ─────────────────────────────────────────────────────

    def batch_download_history(self):
        end_d = datetime.datetime.now().strftime('%Y%m%d')
        start_d = (datetime.datetime.now() - datetime.timedelta(
            days=self.LOOKBACK_CALENDAR_DAYS)).strftime('%Y%m%d')

        print("📅 获取交易日历...")
        cal = _safe_api_call(pro.trade_cal, exchange='SSE',
                             start_date=start_d, end_date=end_d)
        if cal.empty:
            raise Exception("交易日历获取失败")
        trade_dates = cal[cal['is_open'] == 1]['cal_date'].sort_values().tolist()
        print(f"  ✅ 区间 {trade_dates[0]}~{trade_dates[-1]} 共 {len(trade_dates)} 个交易日")

        print("📥 批量下载日线数据...")
        daily_frames = []
        for d in tqdm(trade_dates, desc="  日线"):
            df = _safe_api_call(pro.daily, trade_date=d)
            if not df.empty:
                daily_frames.append(df)
            time.sleep(0.06)

        if not daily_frames:
            raise Exception("日线数据下载失败")
        df_daily = pd.concat(daily_frames, ignore_index=True)

        df_daily = df_daily[df_daily['ts_code'].str[:2].isin(['00', '60'])].copy()
        print(f"  ✅ 日线: {len(df_daily)} 行 ({df_daily['ts_code'].nunique()} 只股票)")

        print("📥 批量下载复权因子...")
        adj_frames = []
        for d in tqdm(trade_dates, desc="  复权"):
            df = _safe_api_call(pro.adj_factor, trade_date=d)
            if not df.empty:
                adj_frames.append(df)
            time.sleep(0.06)

        if adj_frames:
            df_adj = pd.concat(adj_frames, ignore_index=True)
            df_daily = pd.merge(
                df_daily, df_adj[['ts_code', 'trade_date', 'adj_factor']],
                on=['ts_code', 'trade_date'], how='left'
            )
            df_daily = df_daily.sort_values(['ts_code', 'trade_date'])
            df_daily['adj_factor'] = df_daily.groupby('ts_code')['adj_factor'].ffill().bfill()

            latest_adj = df_daily.groupby('ts_code')['adj_factor'].transform('last')
            mask = latest_adj > 0
            factor = np.where(mask, df_daily['adj_factor'] / latest_adj, 1.0)
            for col in ['open', 'high', 'low', 'close', 'pre_close']:
                df_daily[col] = df_daily[col] * factor
            print(f"  ✅ 前复权完成")
        else:
            print("  ⚠️ 复权因子下载失败, 使用未复权价格")

        return df_daily

    # ─────────────────────────────────────────────────────
    # 🧮 向量化筛选 + 诊断模式
    # ─────────────────────────────────────────────────────

    def vectorized_screen(self, df_daily, df_snapshot):
        global filter_stats
        filter_stats = {k: 0 for k in filter_stats}

        current_date = datetime.datetime.now().strftime('%Y%m%d')
        minutes_elapsed = calc_trading_minutes_elapsed()
        vol_completion = max(get_intraday_volume_completion(minutes_elapsed), 0.01)

        # ── 0. 数据准备 ──
        df = df_daily.rename(columns={'vol': 'volume'}).copy()
        df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)

        latest_in_hist = df.groupby('ts_code')['trade_date'].transform('max')
        need_append_codes = df.loc[latest_in_hist < current_date, 'ts_code'].unique()

        snap = df_snapshot.copy()
        snap['ts_code'] = snap['ts_code'].astype(str).str.strip()
        snap_to_append = snap[snap['ts_code'].isin(need_append_codes)].copy()

        if len(snap_to_append) > 0 and minutes_elapsed > 0:
            today_rows = pd.DataFrame({
                'ts_code':    snap_to_append['ts_code'].values,
                'trade_date': current_date,
                'open':       snap_to_append['open'].values,
                'high':       snap_to_append['high'].values,
                'low':        snap_to_append['low'].values,
                'close':      snap_to_append['close'].values,
                'volume':     snap_to_append['volume'].values,
                'pct_chg':    snap_to_append['pct_chg'].values,
            })
            df = pd.concat([df, today_rows], ignore_index=True)
            df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)

        # ── 1. 计算技术指标 ──
        grp = df.groupby('ts_code', sort=False)
        df['MA10']    = grp['close'].transform(lambda x: x.rolling(10, min_periods=10).mean())
        df['MA20']    = grp['close'].transform(lambda x: x.rolling(20, min_periods=20).mean())
        df['MA30']    = grp['close'].transform(lambda x: x.rolling(30, min_periods=30).mean())
        df['Vol_MA5'] = grp['volume'].transform(lambda x: x.rolling(5, min_periods=5).mean())

        df['close_prev']  = grp['close'].shift(1)
        df['MA10_prev']   = grp['MA10'].shift(1)
        df['MA20_prev']   = grp['MA20'].shift(1)
        df['vol_prev']    = grp['volume'].shift(1)
        df['close_20ago'] = grp['close'].shift(19)

        is_today = df['trade_date'] == current_date
        df['vol_projected'] = np.where(
            is_today & (minutes_elapsed < 240),
            df['volume'] / vol_completion,
            df['volume']
        )

        df['vol_avg_5'] = grp['volume'].transform(
            lambda x: x.shift(1).rolling(5, min_periods=1).mean()
        ).fillna(1) + 1
        df['vol_ratio'] = df['vol_projected'] / df['vol_avg_5']

        # ── 2. 爆量标记 ──
        vol_ma5_valid = df['Vol_MA5'].fillna(0) > 0
        df['is_exp'] = (
            (df['pct_chg'] >= EXPLOSION_PCT_MIN) &
            (df['pct_chg'] <= EXPLOSION_PCT_MAX) &
            (df['volume'] > df['Vol_MA5'] * EXPLOSION_VOL_MULT) &
            vol_ma5_valid
        ) | (
            (df['close'] == df['open']) & (df['pct_chg'] > 9.5)
        )

        df['has_exp_16d'] = grp['is_exp'].transform(
            lambda x: x.shift(1).rolling(EXPLOSION_LOOKBACK_DAYS, min_periods=1).max()
        ).fillna(0)

        # ── 3. 多头天数 ──
        df['bull'] = (df['MA10'] > df['MA20']).astype(float)
        df['bull_sum_8d'] = grp['bull'].transform(
            lambda x: x.rolling(BULL_DAYS_WINDOW, min_periods=BULL_DAYS_WINDOW).sum()
        ).fillna(0)

        # ── 4. 取最后一行 ──
        grp_sizes = grp.size()
        valid_stocks = grp_sizes[grp_sizes >= 40].index
        df_valid = df[df['ts_code'].isin(valid_stocks)]
        last = df_valid.groupby('ts_code', sort=False).tail(1).copy()
        last = last.set_index('ts_code')

        filter_stats['0_data_missing'] = int(grp_sizes[grp_sizes < 40].sum()) if (grp_sizes < 40).any() else 0

        # ════════════════════════════════════════════════════
        # 🔬 诊断模式
        # ════════════════════════════════════════════════════
        diag_data = {}
        if self.diag_codes:
            for dc in self.diag_codes:
                if dc in last.index:
                    row = last.loc[dc]
                    ma20_safe = row['MA20'] if row['MA20'] != 0 else np.nan
                    dist_ma = (row['MA10'] - row['MA20']) / ma20_safe if ma20_safe else np.nan
                    dist_ma10 = (row['close'] - row['MA10']) / row['MA10'] if row['MA10'] != 0 else np.nan
                    rise_20d_val = ((row['close'] - row['close_20ago']) / row['close_20ago']
                                    if row['close_20ago'] and row['close_20ago'] > 0 else np.nan)
                    is_pullback = (row['close'] < row['close_prev']) or (row['close'] < row['open'])
                    is_shrink = row['vol_projected'] < row['vol_prev']

                    is_golden = (
                        (row['MA10'] > row['MA20']) and
                        (row['MA20_prev'] >= row['MA10_prev']) and
                        (row['MA10'] > row['MA10_prev'])
                    )

                    diag_data[dc] = {
                        'close': row['close'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'pct_chg': row['pct_chg'],
                        'volume': row['volume'],
                        'vol_projected': row['vol_projected'],
                        'vol_prev': row['vol_prev'],
                        'vol_ratio': row['vol_ratio'],
                        'MA10': row['MA10'],
                        'MA20': row['MA20'],
                        'MA30': row['MA30'],
                        'MA10_prev': row['MA10_prev'],
                        'MA20_prev': row['MA20_prev'],
                        'close_prev': row['close_prev'],
                        'close_20ago': row['close_20ago'],
                        'has_exp_16d': row['has_exp_16d'],
                        'bull_sum_8d': row['bull_sum_8d'],
                        'is_golden_cross': is_golden,
                        'ma20_vs_ma30': row['MA20'] / row['MA30'] if row['MA30'] > 0 else np.nan,
                        'ma10_slope': row['MA10'] / row['MA10_prev'] if row['MA10_prev'] > 0 else np.nan,
                        'close_vs_ma30': row['close'] / row['MA30'] if row['MA30'] > 0 else np.nan,
                        'dist_ma10_ma20': dist_ma,
                        'dist_ma10': dist_ma10,
                        'rise_20d': rise_20d_val,
                        'is_pullback': is_pullback,
                        'is_shrink': is_shrink,
                    }
                elif dc in grp_sizes.index and grp_sizes[dc] < 40:
                    diag_data[dc] = {'_status': f'数据不足 (仅{grp_sizes[dc]}行, 需≥40)'}
                else:
                    diag_data[dc] = {'_status': '未找到该股票 (非主板或代码错误)'}

        # ── 5. 依次应用8条筛选规则 ──
        remaining = pd.Series(True, index=last.index)

        r1 = last['has_exp_16d'] > 0
        filter_stats['8_explode_error'] = int((remaining & ~r1).sum())
        remaining &= r1

        is_stable_bull = last['bull_sum_8d'] >= BULL_DAYS_THRESHOLD
        is_golden_cross = (
            (last['MA10'] > last['MA20']) &
            (last['MA20_prev'] >= last['MA10_prev']) &
            (last['MA10'] > last['MA10_prev'])
        )
        is_ma20_gt_ma30 = last['MA20'] > last['MA30'] * MA20_MA30_TOLERANCE
        r2 = (is_stable_bull | is_golden_cross) & is_ma20_gt_ma30
        filter_stats['2_multi_head_error'] = int((remaining & ~r2).sum())
        remaining &= r2

        r3 = last['MA10'] >= last['MA10_prev'] * MA10_SLOPE_TOLERANCE
        filter_stats['1_trend_error'] = int((remaining & ~r3).sum())
        remaining &= r3

        r4 = last['close'] >= last['MA30'] * MA30_SUPPORT_TOLERANCE
        filter_stats['2_multi_head_error'] += int((remaining & ~r4).sum())
        remaining &= r4

        ma20_safe = last['MA20'].replace(0, np.nan)
        dist_ma = (last['MA10'] - last['MA20']) / ma20_safe
        r5 = (dist_ma <= MAX_DIST_MA10_MA20) & ma20_safe.notna()
        filter_stats['3_ma_dist_error'] = int((remaining & ~r5).sum())
        remaining &= r5

        is_pullback = (last['close'] < last['close_prev']) | (last['close'] < last['open'])
        is_shrink = last['vol_projected'] < last['vol_prev']
        r6 = is_pullback & is_shrink
        filter_stats['4_candle_vol_error'] = int((remaining & ~r6).sum())
        remaining &= r6

        rise_20d = (last['close'] - last['close_20ago']) / last['close_20ago'].replace(0, np.nan)
        rise_20d = rise_20d.fillna(0)
        r7 = rise_20d <= MAX_20_DAYS_RISE
        filter_stats['7_high_pos_error'] = int((remaining & ~r7).sum())
        remaining &= r7

        dist_ma10 = (last['close'] - last['MA10']) / last['MA10']
        r8 = (dist_ma10 > MAX_DIST_MA10_LOW) & (dist_ma10 < MAX_DIST_MA10)
        filter_stats['6_price_pos_error'] = int((remaining & ~r8).sum())
        remaining &= r8

        if self.diag_codes and diag_data:
            self._print_diagnostic_report(diag_data, last, r1, r2, r3, r4, r5, r6, r7, r8, remaining)

        # ── 6. 构建结果 ──
        passed_codes = remaining[remaining].index.tolist()

        snap_indexed = snap.set_index('ts_code')
        results = []
        for code in passed_codes:
            try:
                s = snap_indexed.loc[code]
                row = last.loc[code]

                tags = []
                pre_close_raw = float(s.get('pre_close', 0))
                if pre_close_raw > 0:
                    amp = (float(row['high']) - float(row['low'])) / pre_close_raw
                    if amp > AMP_THRESHOLD and row['vol_projected'] >= row['vol_prev']:
                        tags.append("RISK_高振幅放量")
                vr = float(row['vol_ratio'])
                if vr > TURNOVER_RATIO_LIMIT:
                    tags.append(f"RISK_放量({round(vr, 1)}倍)")

                fs = float(s.get('float_share', 0))
                real_turnover = float(s['volume']) / fs if fs > 0 else 0

                results.append({
                    '代码': code,
                    '名称': s.get('name', ''),
                    '行业': s.get('industry', ''),
                    'circ_mv': float(s.get('circ_mv', 0)),
                    '现价': float(row['close']),
                    '涨幅': round(float(row['pct_chg']), 2),
                    'RiskTags': tags,
                    '相对换手': round(vr, 2),
                    'Turnover': round(real_turnover, 2),
                })
            except:
                pass

        return pd.DataFrame(results) if results else None

    def _print_diagnostic_report(self, diag_data, last, r1, r2, r3, r4, r5, r6, r7, r8, remaining):
        for dc, data in diag_data.items():
            print(f"\n{'='*70}")
            print(f"🔬 诊断报告: {dc}")
            print(f"{'='*70}")

            if '_status' in data:
                print(f"  ❌ {data['_status']}")
                continue

            print(f"\n  📊 基础行情:")
            print(f"     现价={data['close']:.3f}  开盘={data['open']:.3f}  最高={data['high']:.3f}  最低={data['low']:.3f}")
            print(f"     涨幅={data['pct_chg']:.2f}%  昨收={data['close_prev']:.3f}")
            print(f"     成交量={data['volume']:.0f}  预测全天量={data['vol_projected']:.0f}  昨日量={data['vol_prev']:.0f}")
            print(f"     量比={data['vol_ratio']:.2f}")

            print(f"\n  📈 均线数据:")
            print(f"     MA10={data['MA10']:.3f}  MA20={data['MA20']:.3f}  MA30={data['MA30']:.3f}")
            print(f"     MA10(昨)={data['MA10_prev']:.3f}  MA20(昨)={data['MA20_prev']:.3f}")
            print(f"     20日前收盘={data['close_20ago']:.3f}" if not np.isnan(data['close_20ago']) else "     20日前收盘=N/A")

            print(f"\n  🔍 规则逐条判定:")
            print(f"  {'─'*60}")

            rules = []
            in_pool = dc in last.index 

            if not in_pool:
                print(f"  ❌ 该股票不在有效筛选池中")
                continue

            val = data['has_exp_16d']
            passed = bool(r1.get(dc, False)) if dc in r1.index else False
            rules.append(('1_爆量基因', passed, f"has_exp_16d={val:.0f}", f"需>0 (16日内有涨幅{EXPLOSION_PCT_MIN}-{EXPLOSION_PCT_MAX}%且量比>{EXPLOSION_VOL_MULT})"))

            bull_val = data['bull_sum_8d']
            gc_val = data['is_golden_cross']
            ma20_30_ratio = data['ma20_vs_ma30']
            passed = bool(r2.get(dc, False)) if dc in r2.index else False
            detail = f"多头天数={bull_val:.0f}/{BULL_DAYS_THRESHOLD}  金叉={gc_val}  MA20/MA30={ma20_30_ratio:.4f}"
            rules.append(('2_多头排列', passed, detail, f"需{BULL_DAYS_THRESHOLD}/{BULL_DAYS_WINDOW}天或金叉, 且MA20/MA30>{MA20_MA30_TOLERANCE}"))

            slope_val = data['ma10_slope']
            passed = bool(r3.get(dc, False)) if dc in r3.index else False
            rules.append(('3_MA10斜率', passed, f"MA10/MA10昨={slope_val:.5f}", f"需≥{MA10_SLOPE_TOLERANCE}"))

            close_ma30_ratio = data['close_vs_ma30']
            passed = bool(r4.get(dc, False)) if dc in r4.index else False
            rules.append(('4_MA30生命线', passed, f"close/MA30={close_ma30_ratio:.4f}", f"需≥{MA30_SUPPORT_TOLERANCE}"))

            dist_val = data['dist_ma10_ma20']
            passed = bool(r5.get(dc, False)) if dc in r5.index else False
            rules.append(('5_乖离率', passed, f"(MA10-MA20)/MA20={dist_val:.4f}" if dist_val is not None and not np.isnan(dist_val) else "(MA10-MA20)/MA20=N/A",
                          f"需≤{MAX_DIST_MA10_MA20}"))

            pb = data['is_pullback']
            sk = data['is_shrink']
            passed = bool(r6.get(dc, False)) if dc in r6.index else False
            rules.append(('6_阴线+缩量', passed, f"回调={pb}  缩量={sk}", "需同时满足"))

            rise_val = data['rise_20d']
            passed = bool(r7.get(dc, False)) if dc in r7.index else False
            rules.append(('7_防追高', passed, f"20日涨幅={rise_val:.2%}" if rise_val is not None and not np.isnan(rise_val) else "20日涨幅=N/A",
                          f"需≤{MAX_20_DAYS_RISE:.0%}"))

            dist10_val = data['dist_ma10']
            passed = bool(r8.get(dc, False)) if dc in r8.index else False
            rules.append(('8_击球区', passed, f"(close-MA10)/MA10={dist10_val:.4f}" if dist10_val is not None and not np.isnan(dist10_val) else "(close-MA10)/MA10=N/A",
                          f"需在 ({MAX_DIST_MA10_LOW}, {MAX_DIST_MA10}) 即 +{MAX_DIST_MA10_LOW*100}%~+{MAX_DIST_MA10*100}%"))

            first_fail = None
            for name, passed, actual, threshold in rules:
                icon = "✅" if passed else "❌"
                print(f"  {icon} {name:<14} | {actual:<45} | {threshold}")
                if not passed and first_fail is None:
                    first_fail = name

            print(f"  {'─'*60}")
            final_pass = bool(remaining.get(dc, False)) if dc in remaining.index else False
            if final_pass:
                print(f"  🎯 最终结果: ✅ 通过全部8条规则, 入围!")
            else:
                print(f"  🚫 最终结果: ❌ 被淘汰 (首个失败: {first_fail})")
                if first_fail and '击球' in first_fail and dist10_val is not None and not np.isnan(dist10_val):
                    if dist10_val < MAX_DIST_MA10_LOW:
                        gap = MAX_DIST_MA10_LOW - dist10_val
                        print(f"  💡 离击球区下沿差 {gap:.4f} ({gap*100:.2f}%), 股价需再涨 {gap*100:.2f}% 才进入击球区")
                    elif dist10_val >= MAX_DIST_MA10:
                        gap = dist10_val - MAX_DIST_MA10
                        print(f"  💡 超出击球区上沿 {gap:.4f} ({gap*100:.2f}%), 股价需回落 {gap*100:.2f}% 才进入击球区")

            print(f"{'='*70}")

    def run(self):
        t_start = time.time()

        print("\n" + "="*60)
        print("📡 Phase 1/3: 实时行情快照")
        print("="*60)
        df_snapshot = self.get_market_snapshot()
        if df_snapshot is None or df_snapshot.empty:
            print("❌ 无法获取市场快照。终止。")
            return None

        df_target = df_snapshot[df_snapshot['volume'] > 0]
        print(f"  活跃股票: {len(df_target)} 只")

        print("\n" + "="*60)
        print("📥 Phase 2/3: 批量下载历史数据 (10K积分加速)")
        print("="*60)
        try:
            df_daily = self.batch_download_history()
        except Exception as e:
            print(f"❌ 批量下载失败: {e}")
            print("💡 回退到逐只下载模式...")
            return self._fallback_run(df_target)

        print("\n" + "="*60)
        print("🧮 Phase 3/3: 向量化策略筛选")
        print("="*60)
        result_df = self.vectorized_screen(df_daily, df_target)

        elapsed = time.time() - t_start
        print(f"\n⏱️  总耗时: {elapsed:.1f}秒")

        print("\n" + "-"*40)
        print("📊 淘汰漏斗统计")
        print("-"*40)
        sorted_stats = sorted(filter_stats.items(), key=lambda x: x[1], reverse=True)
        for reason, count in sorted_stats:
            if count > 0:
                print(f"  ❌ {reason:<25}: {count}")
        n_pass = len(result_df) if result_df is not None else 0
        print("-"*40)
        print(f"  ✅ 入围: {n_pass}")

        if result_df is not None and not result_df.empty:
            return result_df
        else:
            print("⚠️ 本次扫描未发现符合策略的标的。")
            return None

    def _fallback_run(self, df_target):
        """回退模式: 逐只下载"""
        print("🐌 逐只下载模式 (较慢, 约15-25分钟)...")
        lookback_days = 80
        results = []

        def check_stock(ts_code, snapshot_row):
            try:
                time.sleep(random.uniform(0.01, 0.15))
                end_date = datetime.datetime.now().strftime('%Y%m%d')
                start_date = (datetime.datetime.now() - datetime.timedelta(
                    days=lookback_days + 20)).strftime('%Y%m%d')

                # 适配 ST_CLIENT
                df_hist = _safe_api_call(pro.daily, ts_code=ts_code,
                                         start_date=start_date, end_date=end_date)
                if df_hist.empty or len(df_hist) < 40:
                    with stats_lock: filter_stats["0_data_missing"] += 1
                    return None

                adj = _safe_api_call(pro.adj_factor, ts_code=ts_code,
                                     start_date=start_date, end_date=end_date)
                if not adj.empty:
                    df_hist = pd.merge(df_hist, adj, on='trade_date', how='left')
                    df_hist = df_hist.sort_values('trade_date', ascending=False).reset_index(drop=True)
                    df_hist['adj_factor'] = df_hist['adj_factor'].ffill()
                    latest = df_hist['adj_factor'].iloc[0]
                    if latest > 0:
                        factor = df_hist['adj_factor'] / latest
                        for col in ['close', 'open', 'high', 'low', 'pre_close']:
                            if col in df_hist.columns:
                                df_hist[col] = df_hist[col] * factor
                else:
                    df_hist = df_hist.sort_values('trade_date', ascending=False).reset_index(drop=True)

                return None
            except:
                return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(check_stock, row['ts_code'], row): row['symbol']
                for _, row in df_target.iterrows()
            }
            for f in tqdm(as_completed(futures), total=len(futures), desc="逐只扫描"):
                try:
                    res = f.result()
                    if res:
                        results.append(res)
                except:
                    pass

        return pd.DataFrame(results) if results else None


# ================= 第二阶段: God Tier 资金博弈 =================

class AdvancedSelector:

    def get_moneyflow_bulk(self, ts_code_list):
        print(f"⚡ 批量拉取主力资金流 ({len(ts_code_list)} 只)...")
        if not ts_code_list:
            return pd.DataFrame()

        chunk_size = 50
        all_dfs = []
        end_d = datetime.datetime.now().strftime('%Y%m%d')
        start_d = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y%m%d')

        for i in range(0, len(ts_code_list), chunk_size):
            chunk = ts_code_list[i:i + chunk_size]
            try:
                # 适配 ST_CLIENT
                df = _safe_api_call(pro.moneyflow, ts_code=",".join(chunk),
                                    start_date=start_d, end_date=end_d)
                if df is not None and not df.empty:
                    all_dfs.append(df)
                time.sleep(0.15)
            except:
                pass

        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    def score_stocks(self, candidates_df):
        print("\n>>> 💰 God Tier 资金审计...")
        if candidates_df is None or candidates_df.empty:
            return None

        now_hour = datetime.datetime.now().hour
        is_intraday = 9 <= now_hour < 15

        sector_flow_df = None
        target_date = datetime.datetime.now().strftime('%Y%m%d')
        if is_intraday:
            target_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        for delta in range(5):
            d_str = (datetime.datetime.strptime(target_date, '%Y%m%d') -
                     datetime.timedelta(days=delta)).strftime('%Y%m%d')
            try:
                # 兼容处理: 检查 ST_CLIENT 是否包含显式的 moneyflow_industry 函数
                if hasattr(pro, 'moneyflow_industry'):
                    sdf = _safe_api_call(pro.moneyflow_industry, trade_date=d_str)
                else:
                    # 如果没有直接暴露该方法，尝试走通用底层 _post 请求
                    res = pro._post("/moneyflow_industry", {"trade_date": d_str})
                    sdf = pd.DataFrame(res) if isinstance(res, list) else pd.DataFrame()
                
                if not sdf.empty:
                    sdf['net_inflow_yi'] = sdf['net_amount'] / 1e8
                    sector_flow_df = sdf
                    print(f"  ✅ 板块资金流: {d_str}")
                    break
            except:
                pass

        code_list = candidates_df['代码'].tolist()
        mf_dict = {}
        df_mf = self.get_moneyflow_bulk(code_list)
        if not df_mf.empty:
            for code, group in df_mf.groupby('ts_code'):
                mf_dict[code] = group.head(5)['net_mf_amount'].sum()

        scored = []
        for _, row in candidates_df.iterrows():
            score = 0
            reasons = []
            ts_code = row['代码']
            risk_tags = row['RiskTags']

            sector_is_hot = False
            if sector_flow_df is not None:
                match = sector_flow_df[sector_flow_df['industry'] == row['行业']]
                if not match.empty:
                    net_yi = match.iloc[0]['net_inflow_yi']
                    if net_yi > 5.0:
                        score += 30
                        reasons.append(f"🔥行业爆买({round(net_yi, 1)}亿)")
                        sector_is_hot = True
                    elif net_yi < -5.0:
                        score -= 30
                        reasons.append("⚠️行业出逃")

            risk_penalty = False
            if risk_tags:
                if sector_is_hot:
                    reasons.append("🛡️风口豁免")
                else:
                    score -= 50
                    risk_penalty = True
                    reasons.append("❌形态风险")

            mv_yi = row['circ_mv'] / 10000
            if 20 <= mv_yi <= 150:
                score += 20; reasons.append("💰黄金市值")
            elif mv_yi < 20:
                score += 10; reasons.append("微盘")

            to = row['Turnover']
            if 7 <= to <= 20:
                score += 20; reasons.append("⚡活跃")
            elif to > 25:
                score -= 10; reasons.append(f"过热({to}%)")

            if ts_code in mf_dict:
                net = mf_dict[ts_code]
                if net > 10000:
                    score += 50; reasons.append(f"🐳主力抢筹({int(net / 100)}万)")
                elif net > 3000:
                    score += 20; reasons.append("机构吸纳")
                elif net < -5000:
                    score -= 30; reasons.append("主力出货")

            sugg = "⚪观察"
            if score >= 60:   sugg = "⭐强烈推荐"
            elif score >= 30: sugg = "✅建议关注"
            if risk_penalty and score < 50:
                sugg = "⚠️谨慎"

            scored.append({
                '代码': row['代码'], '名称': row['名称'], '行业': row['行业'],
                '总分': score, '建议': sugg,
                '现价': row['现价'], '涨幅': row['涨幅'], '换手': to,
                '主力资金(5日)': f"{int(mf_dict.get(ts_code, 0) / 100)}万",
                '核心理由': " ".join(reasons),
            })

        return pd.DataFrame(scored).sort_values(by='总分', ascending=False)


# ================= 主程序 =================

if __name__ == "__main__":
    # ── 命令行参数 ──
    parser = argparse.ArgumentParser(description='Quant Master V7.2 Turbo + Diagnostic (ST_CLIENT 版)')
    parser.add_argument('--diag', type=str, default='',
                        help='诊断模式: 逗号分隔的股票代码, 如 --diag 002429,000001,600519')
    args = parser.parse_args()

    # 解析诊断代码
    diag_codes = []
    if args.diag:
        raw = [c.strip() for c in args.diag.split(',') if c.strip()]
        diag_codes = resolve_diag_codes(raw)
        print(f"  🔬 诊断模式已激活: {diag_codes}")

    print(f"\n{'='*65}")
    print(f"  Quant Master V7.2 Turbo  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*65}")
    print(f"  📋 策略参数:")
    print(f"     爆量: {EXPLOSION_LOOKBACK_DAYS}日 涨幅{EXPLOSION_PCT_MIN}-{EXPLOSION_PCT_MAX}% 量比>{EXPLOSION_VOL_MULT}")
    print(f"     多头: {BULL_DAYS_THRESHOLD}/{BULL_DAYS_WINDOW}天 MA20>MA30×{MA20_MA30_TOLERANCE}")
    print(f"     斜率: MA10≥昨日×{MA10_SLOPE_TOLERANCE}  MA30支撑≥×{MA30_SUPPORT_TOLERANCE}")
    print(f"     击球: MA10+{MAX_DIST_MA10_LOW*100}%~+{MAX_DIST_MA10*100}%  乖离<{MAX_DIST_MA10_MA20*100}%")
    print(f"     防追高: 20日涨幅<{MAX_20_DAYS_RISE*100}%")
    print(f"     退出: TP1=+6% TP2=+8% SL=-5% Hold=2天")
    print(f"  🚀 模式: 10K积分批量加速 (预计40-60秒)")
    print(f"{'='*65}\n")

    st = TailEndStrategy(diag_codes=diag_codes)
    df_tech = st.run()

    if df_tech is not None and not df_tech.empty:
        print(f"\n✅ 技术初筛通过: {len(df_tech)} 只")
        sl = AdvancedSelector()
        final = sl.score_stocks(df_tech)

        print("\n" + "="*100)
        cols = ['代码', '名称', '行业', '总分', '建议', '现价', '涨幅', '换手', '核心理由']
        print(final[cols].to_markdown(index=False))
        print("="*100)

        fname = f"GodTier_Selection_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        final.to_csv(fname, index=False, encoding='utf_8_sig')
        print(f"\n✅ 结果已保存: {fname}")
    else:
        print("\n今日无符合条件的个股。")