"""
====================================================================
  Quant Master V7.2 - 策略参数优化器 (Strategy Parameter Optimizer)
====================================================================
  目标: 固定退出规则, 优化13个选股参数的最优组合

  固定退出: TP_D1=+6% | TP_D2/D3=+8% | SL=-5% | 最大持有2天

  架构:
    Phase 0 — 数据缓存: 全市场日线 + 基础特征 → pickle
    Phase 1 — 退出收益预算: 固定退出参数, 对全市场每行预算收益
    Phase 2 — 坐标下降法分3轮优化:
       Round 1: 爆量参数 (lookback, pct_min, pct_max, vol_mult)
       Round 2: 均线排列 (bull_window, bull_threshold, tolerances)
       Round 3: 距离/位置 (ma10距离, 乖离率, 涨幅上限)
    Phase 3 — 联合精修: 3轮最优合并 → 邻域搜索抓参数间交互
    Phase 4 — Walk-Forward 验证

  核心优化:
    - 退出收益只算一次 (覆盖全市场所有行, 信号筛选只需取子集)
    - 爆量滚动用纯numpy lag累加, 不用pandas groupby (快100x)
    - 纯阈值参数搜索零重复计算
====================================================================
"""

import tushare as ts
import pandas as pd
import numpy as np
import datetime, time, os, pickle, itertools
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# ╔══════════════════════════════════════════════════════════════╗
# ║                     配置区                                   ║
# ╚══════════════════════════════════════════════════════════════╝

# --- 回测区间 ---
BACKTEST_START = '20210220'
BACKTEST_END   = '20260220'

# --- 固定退出参数 (你的最优解) ---
FIXED_TP1  = 0.06
FIXED_TP2  = 0.08
FIXED_TP3  = 0.08
FIXED_SL   = -0.05
FIXED_HOLD = 2

# --- 交易成本 ---
COMMISSION_RATE = 0.00025
STAMP_TAX_RATE  = 0.0005
SLIPPAGE_RATE   = 0.0005
TOTAL_COST = COMMISSION_RATE * 2 + STAMP_TAX_RATE + SLIPPAGE_RATE * 2

# --- 默认策略参数 (当前V7.2, 作为坐标下降起点) ---
DEFAULT_PARAMS = {
    'lookback':      14,
    'pct_min':       8.0,
    'pct_max':       20.0,
    'vol_mult':      1.2,
    'bull_window':   4,
    'bull_threshold': 3,
    'ma20_30_tol':   0.995,
    'ma10_slope_tol': 0.999,
    'ma30_support':  0.99,
    'ma10_high':     0.02,
    'ma10_low':      -0.01,
    'ma10_ma20_dist': 0.04,
    'max_20d_rise':  0.60,
}

# --- 搜索网格 ---
# Round 1: 爆量参数
GRID_R1 = {
    'lookback':  [6, 8, 10, 12, 14, 18, 20],
    'pct_min':   [5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
    'pct_max':   [15.0, 18.0, 20.0, 25.0, 30.0],
    'vol_mult':  [0.8, 1.0, 1.2, 1.5, 2.0],
}
# Round 2: 均线排列
GRID_R2 = {
    'bull_window':    [3, 4, 5, 6],
    'bull_threshold': [2, 3, 4],    # 自动约束 <= window
    'ma20_30_tol':    [0.985, 0.99, 0.995, 1.0],
    'ma10_slope_tol': [0.995, 0.997, 0.999, 1.0, 1.001],
}
# Round 3: 距离/位置
GRID_R3 = {
    'ma10_high':      [0.015, 0.02, 0.025, 0.03, 0.04, 0.05],
    'ma10_low':       [-0.025, -0.02, -0.015, -0.01, -0.005, 0.0],
    'ma10_ma20_dist': [0.03, 0.04, 0.05, 0.06, 0.08],
    'ma30_support':   [0.96, 0.97, 0.98, 0.99, 1.0],
    'max_20d_rise':   [0.30, 0.40, 0.50, 0.60, 0.80, 1.00],
}

MIN_TRADES    = 30
TRAIN_RATIO   = 0.70
CACHE_DIR     = './qm_cache'
DOWNLOAD_SLEEP = 0.12
WARMUP_CALENDAR_DAYS = 150
BUFFER_CALENDAR_DAYS = 25

TP_MAP = {1: FIXED_TP1, 2: FIXED_TP2}

# ╔══════════════════════════════════════════════════════════════╗
# ║                     Tushare 初始化                           ║
# ╚══════════════════════════════════════════════════════════════╝
print("=" * 65)
print("  🧬 Quant Master V7.2 — Strategy Parameter Optimizer")
print("=" * 65)

pro = ts.pro_api('init_placeholder')
pro._DataApi__token    = '4502048184048033923'
pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'
print("✅ 数据通道已激活")

os.makedirs(CACHE_DIR, exist_ok=True)


# ╔══════════════════════════════════════════════════════════════╗
# ║                 Phase 0: 数据 + 特征缓存                     ║
# ╚══════════════════════════════════════════════════════════════╝
class FeatureCache:

    def __init__(self, bt_start, bt_end):
        self.bt_start, self.bt_end = bt_start, bt_end
        self.key = f"{bt_start}_{bt_end}"
        self.feat_path = os.path.join(CACHE_DIR, f"features_{self.key}.pkl")
        self.daily_path = os.path.join(CACHE_DIR, f"daily_{self.key}.pkl")

    def load(self):
        if os.path.exists(self.feat_path):
            print(f"⚡ 加载特征缓存: {self.feat_path}")
            with open(self.feat_path, 'rb') as f:
                d = pickle.load(f)
            print(f"   {len(d['df']):,} 行  {len(d['trade_dates'])} 交易日  ⏩ 直接优化\n")
            return d['df'], d['trade_dates']

        if os.path.exists(self.daily_path):
            print(f"⚡ 加载日线缓存: {self.daily_path}")
            with open(self.daily_path, 'rb') as f:
                c = pickle.load(f)
            df, trade_dates = c['df'], c['trade_dates']
        else:
            df, trade_dates = self._download()
            with open(self.daily_path, 'wb') as f:
                pickle.dump({'df': df, 'trade_dates': trade_dates}, f)
            print(f"💾 日线缓存 → {self.daily_path}")

        print("⚙️  复权 + 特征...")
        df = self._adj_prices(df)
        df = self._features(df)
        df = self._future_prices(df)

        with open(self.feat_path, 'wb') as f:
            pickle.dump({'df': df, 'trade_dates': trade_dates}, f)
        print(f"💾 特征缓存 → {self.feat_path}\n")
        return df, trade_dates

    def _download(self):
        ws = (datetime.datetime.strptime(self.bt_start, '%Y%m%d')
              - datetime.timedelta(days=WARMUP_CALENDAR_DAYS)).strftime('%Y%m%d')
        be = (datetime.datetime.strptime(self.bt_end, '%Y%m%d')
              + datetime.timedelta(days=BUFFER_CALENDAR_DAYS)).strftime('%Y%m%d')
        cal = pro.trade_cal(exchange='SSE', start_date=ws, end_date=be)
        od = sorted(cal[cal['is_open'] == 1]['cal_date'].tolist())
        td = [d for d in od if self.bt_start <= d <= self.bt_end]
        print(f"  回测: {len(td)} 天  预热+缓冲: {len(od)} 天")
        dfs = []
        for date in tqdm(od, desc="下载", ncols=80):
            for _ in range(3):
                try:
                    r = pro.daily(trade_date=date)
                    if r is not None and not r.empty:
                        dfs.append(r); break
                except: time.sleep(0.5)
            time.sleep(DOWNLOAD_SLEEP)
        df = pd.concat(dfs, ignore_index=True)
        df = df[df['ts_code'].str[:2].isin(['60', '00'])].copy()
        df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
        print(f"  主板: {len(df):,} 行 ({df['ts_code'].nunique()} 只)")
        return df, td

    def _adj_prices(self, df):
        def _adj(g):
            g = g.sort_values('trade_date').copy()
            cr = np.cumprod(1 + g['pct_chg'].fillna(0).values / 100)
            lc, lr = g['close'].values[-1], cr[-1]
            if lr == 0 or np.isnan(lr):
                g['adj_close']=g['close']; g['adj_open']=g['open']
                g['adj_high']=g['high']; g['adj_low']=g['low']; return g
            g['adj_close'] = lc * cr / lr
            f = (g['adj_close'] / g['close'].replace(0, np.nan)).fillna(1)
            g['adj_open']=g['open']*f; g['adj_high']=g['high']*f; g['adj_low']=g['low']*f
            return g
        tqdm.pandas(desc="复权", ncols=80)
        return df.groupby('ts_code', group_keys=False).progress_apply(_adj)

    def _features(self, df):
        g = df.groupby('ts_code')
        df['MA10'] = g['adj_close'].transform(lambda x: x.rolling(10,10).mean())
        df['MA20'] = g['adj_close'].transform(lambda x: x.rolling(20,20).mean())
        df['MA30'] = g['adj_close'].transform(lambda x: x.rolling(30,30).mean())
        df['Vol_MA5'] = g['vol'].transform(lambda x: x.rolling(5,5).mean())
        df['yest_close'] = g['adj_close'].shift(1)
        df['yest_volume'] = g['vol'].shift(1)
        df['yest_MA10'] = g['MA10'].shift(1)
        df['yest_MA20'] = g['MA20'].shift(1)
        df['close_20d_ago'] = g['adj_close'].shift(19)
        df['is_golden_cross'] = (
            (df['MA10']>df['MA20']) & (df['yest_MA10']<=df['yest_MA20']) & (df['MA10']>df['yest_MA10']))
        return df

    def _future_prices(self, df):
        g = df.groupby('ts_code')
        for d in range(1, max(FIXED_HOLD, 5) + 1):
            df[f'next{d}_open'] =g['adj_open'].shift(-d)
            df[f'next{d}_high'] =g['adj_high'].shift(-d)
            df[f'next{d}_low']  =g['adj_low'].shift(-d)
            df[f'next{d}_close']=g['adj_close'].shift(-d)
        return df


# ╔══════════════════════════════════════════════════════════════╗
# ║              Phase 1: 策略优化器 (核心引擎)                   ║
# ╚══════════════════════════════════════════════════════════════╝
class StrategyOptimizer:

    def __init__(self, df, trade_dates):
        self.n = len(df)
        self.trade_dates_arr = df['trade_date'].values
        self.in_range = np.isin(self.trade_dates_arr,  trade_dates)
        self.trade_dates_list = sorted(trade_dates)

        print("  📐 预算退出收益 (固定退出参数)...")
        self.exit_ret = self._precompute_exits(df)
        valid_exits = (~np.isnan(self.exit_ret) & self.in_range).sum()
        print(f"     有效退出: {valid_exits:,} / {self.in_range.sum():,}")

        # 特征数组 (numpy)
        self.pct_chg    = df['pct_chg'].values.astype(float)
        self.vol        = df['vol'].values.astype(float)
        self.Vol_MA5    = df['Vol_MA5'].values.astype(float)
        self.adj_close  = df['adj_close'].values.astype(float)
        self.adj_open   = df['adj_open'].values.astype(float)
        self.MA10       = df['MA10'].values.astype(float)
        self.MA20       = df['MA20'].values.astype(float)
        self.MA30       = df['MA30'].values.astype(float)
        self.yest_close = df['yest_close'].values.astype(float)
        self.yest_vol   = df['yest_volume'].values.astype(float)
        self.yest_MA10  = df['yest_MA10'].values.astype(float)
        self.yest_MA20  = df['yest_MA20'].values.astype(float)
        self.close_20d  = df['close_20d_ago'].values.astype(float)
        self.golden_x   = df['is_golden_cross'].values.astype(bool)
        self.raw_close  = df['close'].values.astype(float)
        self.raw_open   = df['open'].values.astype(float)

        # 组结构 (用于快速滚动)
        gids = df.groupby('ts_code').ngroup().values
        self.grp_change = np.concatenate([[True], gids[1:] != gids[:-1]])
        self.pos = self._calc_pos()

        # vol_ratio + limit_up (不随参数变化)
        vm5_safe = np.where(self.Vol_MA5 > 0, self.Vol_MA5, np.nan)
        self.vol_ratio = self.vol / vm5_safe
        self.is_limit_up = ((self.raw_close == self.raw_open) & (self.pct_chg > 9.5))

        # 预算多头排列列 (不同窗口)
        print("  📐 预算多头排列列...")
        bf = (self.MA10 > self.MA20).astype(float)
        self.bull_cols = {}
        for w in sorted(set([3, 4, 5, 6] + [v for v in GRID_R2.get('bull_window', [])])):
            self.bull_cols[w] = self._rolling_sum(bf, w, min_p=w)
            print(f"     bull_{w}d ✓")

        # 预算可复用的比值 (避免重复除法)
        m20_safe = np.where(self.MA20 > 0, self.MA20, np.nan)
        m10_safe = np.where(self.MA10 > 0, self.MA10, np.nan)
        c20_safe = np.where(self.close_20d > 0, self.close_20d, np.nan)

        self.ma_spread  = (self.MA10 - self.MA20) / m20_safe      # MA10-MA20 乖离
        self.dist_ma10  = (self.adj_close - self.MA10) / m10_safe  # 股价vs MA10
        self.rise_20d   = (self.adj_close - self.close_20d) / c20_safe

        # 阴线+缩量 (不随参数变化)
        self.f6 = ((self.adj_close < self.yest_close) |
                   (self.adj_close < self.adj_open)) & (self.vol < self.yest_vol)

        print(f"  ✅ 优化器就绪 ({self.n:,} 行)\n")

    # ---------- 组内位置 ----------
    def _calc_pos(self):
        pos = np.zeros(self.n, dtype=np.int32)
        p = 0
        for i in range(self.n):
            if self.grp_change[i]: p = 0
            pos[i] = p
            p += 1
        return pos

    # ---------- 快速滚动求和 (numpy lag累加) ----------
    def _rolling_sum(self, vals, window, min_p=1):
        n = len(vals)
        total = np.zeros(n, dtype=float)
        count = np.zeros(n, dtype=float)
        for lag in range(window):
            s = np.zeros(n)
            if lag == 0:
                s[:] = vals
            else:
                s[lag:] = vals[:-lag]
            v = self.pos >= lag
            total += s * v
            count += v.astype(float)
        return np.where(count >= min_p, total, np.nan)

    # ---------- 快速爆量检测 (numpy lag累加) ----------
    def _has_explosion(self, pct_min, pct_max, vol_mult, lookback):
        lookback = int(lookback)  # <--- 新增此行，强制转为整型

        is_exp = ((self.pct_chg >= pct_min) & (self.pct_chg <= pct_max) &
                  (self.vol_ratio > vol_mult)) | self.is_limit_up
        is_exp = is_exp.astype(float)
        n = self.n
        result = np.zeros(n, dtype=bool)
        for lag in range(1, lookback + 1):
            s = np.zeros(n)
            s[lag:] = is_exp[:-lag]
            result |= (s > 0) & (self.pos >= lag)
        return result

    # ---------- 预算退出收益 ----------
    def _precompute_exits(self, df):
        entry = df['adj_close'].values.astype(float)
        entry_safe = np.where(entry > 0, entry, np.nan)
        n = len(df)
        ret = {}
        for d in range(1, FIXED_HOLD + 1):
            for f in ['open', 'high', 'low', 'close']:
                col = f'next{d}_{f}'
                ret[f'd{d}_{f}'] = df[col].values / entry_safe - 1 if col in df.columns \
                    else np.full(n, np.nan)

        exit_ret = np.full(n, np.nan)
        exited = np.zeros(n, dtype=bool)

        for day in range(1, FIXED_HOLD + 1):
            tp = TP_MAP.get(day, FIXED_TP3)
            do, dh, dl, dc = ret[f'd{day}_open'], ret[f'd{day}_high'], \
                             ret[f'd{day}_low'],  ret[f'd{day}_close']
            vld = ~exited & ~np.isnan(do)

            m = vld & (do >= tp);    exit_ret[m]=do[m]; exited[m]=True; vld &= ~m
            m = vld & (do <= FIXED_SL); exit_ret[m]=do[m]; exited[m]=True; vld &= ~m
            ht, hs = dh >= tp, dl <= FIXED_SL
            m = vld & ht & hs;      exit_ret[m]=FIXED_SL; exited[m]=True; vld &= ~m
            m = vld & ht;           exit_ret[m]=tp;       exited[m]=True; vld &= ~m
            m = vld & hs;           exit_ret[m]=FIXED_SL; exited[m]=True; vld &= ~m
            if day == FIXED_HOLD:
                m = vld & ~np.isnan(dc); exit_ret[m]=dc[m]; exited[m]=True

        return exit_ret - TOTAL_COST

    # ---------- 信号生成 + 评估 ----------
    def evaluate(self, params, date_mask=None):
        """
        给定参数字典, 生成信号 → 取子集退出收益 → 计算绩效
        date_mask: 可选, 进一步限定日期范围 (Walk-Forward用)
        """
        # 强制转换整型参数以防 dataframe 导致的数据类型变更
        lookback = int(params['lookback'])
        bull_window = int(params['bull_window'])
        bull_threshold = int(params['bull_threshold'])

        f1 = self._has_explosion(
            params['pct_min'], params['pct_max'],
            params['vol_mult'], lookback)

        bull = self.bull_cols.get(bull_window)
        if bull is None:
            bf = (self.MA10 > self.MA20).astype(float)
            bull = self._rolling_sum(bf, bull_window, min_p=bull_window)
        
        f2_stable = bull >= bull_threshold
        f2 = (f2_stable | self.golden_x) & (self.MA20 > self.MA30 * params['ma20_30_tol'])

        f3 = self.MA10 >= self.yest_MA10 * params['ma10_slope_tol']
        f4 = self.adj_close >= self.MA30 * params['ma30_support']
        f5 = self.ma_spread < params['ma10_ma20_dist']
        f7 = self.rise_20d < params['max_20d_rise']
        f8 = (self.dist_ma10 > params['ma10_low']) & (self.dist_ma10 < params['ma10_high'])

        base = self.in_range if date_mask is None else (self.in_range & date_mask)
        mask = base & f1 & f2 & f3 & f4 & f5 & self.f6 & f7 & f8

        ret = self.exit_ret[mask]
        valid = ~np.isnan(ret)
        ret = ret[valid]
        n = len(ret)
        if n < MIN_TRADES:
            return None

        wins = ret > 0
        nw = wins.sum()
        wr = nw / n
        aw = ret[wins].mean() if nw > 0 else 0
        al = ret[~wins].mean() if (n - nw) > 0 else 0
        exp = wr * aw + (1 - wr) * al
        cum = np.cumprod(1 + ret)
        tr = cum[-1] - 1
        pk = np.maximum.accumulate(cum)
        mdd = ((cum - pk) / pk).min()
        score = exp * np.sqrt(min(n, 500))

        return {
            'n_trades': n, 'win_rate': wr, 'avg_win': aw, 'avg_loss': al,
            'expectancy': exp, 'total_ret': tr, 'max_dd': mdd, 'score': score,
        }

    # ---------- 网格搜索 (某组参数) ----------
    def search_round(self, grid, fixed_params, desc="搜索",
                     date_mask=None, constraints=None):
        keys = list(grid.keys())
        combos = itertools.product(*[grid[k] for k in keys])
        total_iters = np.prod([len(grid[k]) for k in keys]) # 计算总数用于进度条

        results = []
        for vals in tqdm(combos, total=total_iters, desc=desc, ncols=80):
            p = fixed_params.copy()
            p.update(dict(zip(keys, vals)))

            # 约束: bull_threshold <= bull_window
            if 'bull_threshold' in p and 'bull_window' in p:
                if p['bull_threshold'] > p['bull_window']:
                    continue
            # 约束: ma10_low < ma10_high
            if 'ma10_low' in p and 'ma10_high' in p:
                if p['ma10_low'] >= p['ma10_high']:
                    continue

            if constraints and not constraints(p):
                continue

            m = self.evaluate(p, date_mask=date_mask)
            if m is not None:
                for k, v in zip(keys, vals):
                    m[k] = v
                results.append(m)

        df = pd.DataFrame(results)
        if df.empty:
            return df
        df = df.sort_values('score', ascending=False).reset_index(drop=True)
        return df

    # ---------- 精细搜索 ----------
    def refine(self, best_params, grid_keys, step_map, fixed_params, desc="精修"):
        """在最优点±附近以更细步长搜索"""
        fine_grid = {}
        for k in grid_keys:
            center = best_params[k]
            step = step_map.get(k, 0.005)
            if isinstance(center, int):
                fine_grid[k] = sorted(set([max(1, center - 1), center, center + 1]))
            else:
                pts = np.arange(center - step * 4, center + step * 4.5, step)
                pts = [round(p, 4) for p in pts if p > 0 or k in ('ma10_low', 'ma30_support')]
                if k == 'ma10_low':
                    pts = [round(p, 4) for p in np.arange(center - step*4, center + step*4.5, step)]
                fine_grid[k] = sorted(set(pts))

        return self.search_round(fine_grid, fixed_params, desc=desc)


# ╔══════════════════════════════════════════════════════════════╗
# ║                     报告打印器                               ║
# ╚══════════════════════════════════════════════════════════════╝
def print_results(df, title, param_keys, top_n=12):
    if df is None or df.empty:
        print(f"  {title}: 无结果"); return
    n_show = min(top_n, len(df))
    print(f"\n{'='*80}")
    print(f"  {title} (TOP {n_show})")
    print(f"{'='*80}")

    # 动态表头
    pk_short = [k[:8] for k in param_keys]
    hdr = f"  {'#':>3} " + " ".join(f"{s:>8}" for s in pk_short)
    hdr += f" {'胜率':>6} {'期望':>7} {'累计':>8} {'回撤':>7} {'笔数':>5} {'评分':>7}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    for i, row in df.head(n_show).iterrows():
        rank = i + 1
        vals = " ".join(f"{row[k]:>8.3f}" if isinstance(row[k], float)
                        else f"{int(row[k]):>8}" for k in param_keys)
        wr = row['win_rate'] * 100
        exp = row['expectancy'] * 100
        tr = row['total_ret'] * 100
        dd = row['max_dd'] * 100
        n = int(row['n_trades'])
        sc = row['score']
        medal = '🥇' if rank==1 else '🥈' if rank==2 else '🥉' if rank==3 else '  '
        print(f"  {medal}{rank:>2} {vals}"
              f" {wr:>5.1f}% {exp:>+6.2f}% {tr:>+7.1f}% {dd:>6.1f}% {n:>5} {sc:>6.3f}")
    print("  " + "-" * (len(hdr) - 2))


def print_sensitivity(opt, best_params, param_keys, step_map):
    print(f"\n{'='*80}")
    print(f"  📊 参数敏感度分析")
    print(f"{'='*80}")
    for k in param_keys:
        center = best_params[k]
        step = step_map.get(k, 0.005)
        if isinstance(center, int):
            values = range(max(1, center - 2), center + 3)
        else:
            values = np.arange(center - step*5, center + step*5.5, step).round(4)
        print(f"\n  🔍 {k}:")
        for v in values:
            p = best_params.copy()
            p[k] = v
            if 'bull_threshold' in p and 'bull_window' in p and p['bull_threshold'] > p['bull_window']:
                continue
            m = opt.evaluate(p)
            if m is None: continue
            mark = ' ◀── 当前' if (abs(v - center) < 0.0001 if isinstance(center, float)
                                    else v == center) else ''
            vfmt = f"{v:>8.3f}" if isinstance(v, float) else f"{v:>8}"
            bl = max(0, int(m['expectancy'] * 800))
            bar = '█' * min(bl, 30)
            print(f"     {vfmt}  期望:{m['expectancy']*100:>+5.2f}%"
                  f"  胜率:{m['win_rate']*100:>5.1f}%  笔数:{m['n_trades']:>4}  {bar}{mark}")


# ╔══════════════════════════════════════════════════════════════╗
# ║                        主程序                               ║
# ╚══════════════════════════════════════════════════════════════╝
if __name__ == '__main__':

    print(f"\n  📅 区间: {BACKTEST_START} ~ {BACKTEST_END}")
    print(f"  🔒 固定退出: TP1={FIXED_TP1*100}% TP2={FIXED_TP2*100}%"
          f" SL={FIXED_SL*100}% Hold={FIXED_HOLD}天")
    r1n = np.prod([len(v) for v in GRID_R1.values()])
    r2n = np.prod([len(v) for v in GRID_R2.values()])
    r3n = np.prod([len(v) for v in GRID_R3.values()])
    print(f"  🔧 网格: R1={r1n} R2={r2n} R3={r3n} 总≈{r1n+r2n+r3n}\n")

    # ========== Phase 0: 数据 ==========
    fc = FeatureCache(BACKTEST_START, BACKTEST_END)
    df, trade_dates = fc.load()

    # ========== Phase 1: 初始化优化器 ==========
    opt = StrategyOptimizer(df, trade_dates)

    # 基准绩效
    base_m = opt.evaluate(DEFAULT_PARAMS)
    if base_m:
        print(f"  📍 基准 (V7.2默认参数):")
        print(f"     胜率:{base_m['win_rate']*100:.1f}%  期望:{base_m['expectancy']*100:+.2f}%"
              f"  累计:{base_m['total_ret']*100:+.1f}%  笔数:{base_m['n_trades']}")
    else:
        print("  ⚠️ 基准参数无信号\n")

    # ============================================================
    #  Phase 2: 坐标下降 - Round 1 (爆量参数)
    # ============================================================
    print(f"\n{'='*65}")
    print(f"  🔥 Round 1: 爆量参数优化")
    print(f"     lookback × pct_min × pct_max × vol_mult")
    print(f"{'='*65}")

    r1_fixed = DEFAULT_PARAMS.copy()
    r1_results = opt.search_round(GRID_R1, r1_fixed, desc="R1-爆量")
    r1_keys = list(GRID_R1.keys())
    print_results(r1_results, "Round 1: 爆量参数", r1_keys)

    if r1_results.empty:
        print("❌ Round 1 无结果, 退出。"); exit()

    best_r1 = {k: r1_results.iloc[0][k] for k in r1_keys}
    best_so_far = DEFAULT_PARAMS.copy()
    best_so_far.update(best_r1)
    print(f"\n  ✅ R1最优: {best_r1}")

    # ============================================================
    #  Phase 2: 坐标下降 - Round 2 (均线排列)
    # ============================================================
    print(f"\n{'='*65}")
    print(f"  📐 Round 2: 均线排列优化")
    print(f"     bull_window × bull_threshold × ma20_30_tol × ma10_slope_tol")
    print(f"{'='*65}")

    r2_results = opt.search_round(GRID_R2, best_so_far, desc="R2-均线")
    r2_keys = list(GRID_R2.keys())
    print_results(r2_results, "Round 2: 均线排列", r2_keys)

    if not r2_results.empty:
        best_r2 = {k: r2_results.iloc[0][k] for k in r2_keys}
        best_so_far.update(best_r2)
        print(f"\n  ✅ R2最优: {best_r2}")

    # ============================================================
    #  Phase 2: 坐标下降 - Round 3 (距离/位置)
    # ============================================================
    print(f"\n{'='*65}")
    print(f"  📏 Round 3: 距离/位置优化")
    print(f"     ma10距离 × 乖离率 × MA30支撑 × 涨幅限制")
    print(f"{'='*65}")

    r3_results = opt.search_round(GRID_R3, best_so_far, desc="R3-距离")
    r3_keys = list(GRID_R3.keys())
    print_results(r3_results, "Round 3: 距离/位置", r3_keys)

    if not r3_results.empty:
        best_r3 = {k: r3_results.iloc[0][k] for k in r3_keys}
        best_so_far.update(best_r3)
        print(f"\n  ✅ R3最优: {best_r3}")

# ============================================================
    #  Phase 3: 降维模块化精修
    # ============================================================
    print(f"\n{'='*65}")
    print(f"  🔬 Phase 3: 模块化细步长精修")
    print(f"{'='*65}")

    step_map = {
        'lookback': 1, 'pct_min': 0.5, 'pct_max': 1.0, 'vol_mult': 0.1,
        'bull_window': 1, 'bull_threshold': 1,
        'ma20_30_tol': 0.002, 'ma10_slope_tol': 0.001,
        'ma10_high': 0.005, 'ma10_low': 0.005,
        'ma10_ma20_dist': 0.005, 'ma30_support': 0.005, 'max_20d_rise': 0.05,
    }
    
    # 拆分成三个独立的细化域，避免 OOM 和算力爆炸
    fine_groups = [
        ("爆量参数", list(GRID_R1.keys())),
        ("均线排列", list(GRID_R2.keys())),
        ("距离位置", list(GRID_R3.keys()))
    ]

    for name, keys in fine_groups:
        print(f"\n  🔍 精修模块: {name} ({len(keys)} 个参数)")
        # 针对当前组生成细化网格
        fine_results = opt.refine(best_so_far, keys, step_map, best_so_far, desc=f"精修-{name}")
        
        if not fine_results.empty:
            # 动态更新最优解
            for k in keys:
                best_so_far[k] = fine_results.iloc[0][k]
            
            # 打印当前精修组的最优结果
            print_results(fine_results, f"{name} 精修结果", keys, top_n=3)

    print(f"\n  ✅ 最终精修最优参数: {best_so_far}")

    # ============================================================
    #  Phase 4: Walk-Forward 验证
    # ============================================================
    print(f"\n{'='*65}")
    print(f"  🧪 Phase 4: Walk-Forward ({int(TRAIN_RATIO*100)}/{int((1-TRAIN_RATIO)*100)})")
    print(f"{'='*65}")

    split = int(len(trade_dates) * TRAIN_RATIO)
    train_d = trade_dates[:split]
    test_d  = trade_dates[split:]
    print(f"  样本内: {train_d[0]}~{train_d[-1]} ({len(train_d)}天)")
    print(f"  样本外: {test_d[0]}~{test_d[-1]} ({len(test_d)}天)")

    train_mask = np.isin(opt.trade_dates_arr, train_d)
    test_mask  = np.isin(opt.trade_dates_arr, test_d)

    # 样本内搜索 (用粗网格R1, 然后级联)
    print(f"\n  🔬 样本内优化...")
    tr_r1 = opt.search_round(GRID_R1, DEFAULT_PARAMS, "IS-R1", date_mask=train_mask)
    if tr_r1.empty:
        print("  ⚠️ 样本内 R1 无结果")
    else:
        is_best = DEFAULT_PARAMS.copy()
        is_best.update({k: tr_r1.iloc[0][k] for k in GRID_R1.keys()})
        tr_r2 = opt.search_round(GRID_R2, is_best, "IS-R2", date_mask=train_mask)
        if not tr_r2.empty:
            is_best.update({k: tr_r2.iloc[0][k] for k in GRID_R2.keys()})
        tr_r3 = opt.search_round(GRID_R3, is_best, "IS-R3", date_mask=train_mask)
        if not tr_r3.empty:
            is_best.update({k: tr_r3.iloc[0][k] for k in GRID_R3.keys()})

        # 样本外验证
        print(f"\n  🧪 样本外验证...")
        is_m = opt.evaluate(is_best, date_mask=train_mask)
        oos_m = opt.evaluate(is_best, date_mask=test_mask)

        # 也验证全量最优
        full_m_is  = opt.evaluate(best_so_far, date_mask=train_mask)
        full_m_oos = opt.evaluate(best_so_far, date_mask=test_mask)

        print(f"\n  {'来源':<18} {'IS胜率':>7} {'IS期望':>8} {'OOS胜率':>8} {'OOS期望':>9} {'衰减':>6}")
        print("  " + "-" * 60)
        for label, is_metric, oos_metric in [
            ("样本内最优", is_m, oos_m),
            ("全量精修最优", full_m_is, full_m_oos)
        ]:
            if is_metric and oos_metric:
                ie, oe = is_metric['expectancy']*100, oos_metric['expectancy']*100
                decay = ((oe - ie) / abs(ie) * 100) if ie != 0 else 0
                flag = '🟢' if decay > -20 else '🟡' if decay > -50 else '🔴'
                print(f"  {label:<18} {is_metric['win_rate']*100:>6.1f}% {ie:>+7.2f}%"
                      f" {oos_metric['win_rate']*100:>7.1f}% {oe:>+8.2f}% {flag}{decay:>+5.0f}%")
            elif is_metric:
                print(f"  {label:<18} {is_metric['win_rate']*100:>6.1f}% "
                      f"{is_metric['expectancy']*100:>+7.2f}%  (样本外信号不足)")
        print("  " + "-" * 60)
        print("  🟢<20%=稳健  🟡20-50%=可接受  🔴>50%=过拟合")

    # ============================================================
    #  最终推荐
    # ============================================================
    final = best_so_far
    final_m = opt.evaluate(final)

    print(f"\n{'='*65}")
    print(f"  🏆 最终推荐参数")
    print(f"{'='*65}")
    print(f"""
  ┌───────────────────────────────────────────────────┐
  │ 爆量参数                                          │
  │   EXPLOSION_LOOKBACK_DAYS = {int(final['lookback']):<24}│
  │   EXPLOSION_PCT_MIN       = {final['pct_min']:<24}│
  │   EXPLOSION_PCT_MAX       = {final['pct_max']:<24}│
  │   EXPLOSION_VOL_MULT      = {final['vol_mult']:<24}│
  │                                                   │
  │ 均线排列                                          │
  │   BULL_DAYS_WINDOW        = {int(final['bull_window']):<24}│
  │   BULL_DAYS_THRESHOLD     = {int(final['bull_threshold']):<24}│
  │   MA20_MA30_TOLERANCE     = {final['ma20_30_tol']:<24}│
  │   MA10_SLOPE_TOLERANCE    = {final['ma10_slope_tol']:<24}│
  │                                                   │
  │ 距离/位置                                          │
  │   MAX_DIST_MA10           = {final['ma10_high']:<24}│
  │   MAX_DIST_MA10_LOW       = {final['ma10_low']:<24}│
  │   MAX_DIST_MA10_MA20      = {final['ma10_ma20_dist']:<24}│
  │   MA30_SUPPORT_TOLERANCE  = {final['ma30_support']:<24}│
  │   MAX_20_DAYS_RISE        = {final['max_20d_rise']:<24}│
  ├───────────────────────────────────────────────────┤""")
    if final_m:
        print(f"""  │ 绩效                                             │
  │   胜率      : {final_m['win_rate']*100:.1f}%{' '*35}│
  │   单笔期望  : {final_m['expectancy']*100:+.2f}%{' '*34}│
  │   累计收益  : {final_m['total_ret']*100:+.1f}%{' '*33}│
  │   最大回撤  : {final_m['max_dd']*100:.1f}%{' '*35}│
  │   交易笔数  : {int(final_m['n_trades'])}{' '*37}│""")
    print(f"  └───────────────────────────────────────────────────┘")

    # ========== 保存 ==========
    tag = f"{BACKTEST_START}_{BACKTEST_END}"
    # if not r1_results.empty:
    #     r1_results.to_csv(f"SOPT_R1_{tag}.csv", index=False)
    # if not r2_results.empty:
    #     r2_results.to_csv(f"SOPT_R2_{tag}.csv", index=False)
    # if not r3_results.empty:
    #     r3_results.to_csv(f"SOPT_R3_{tag}.csv", index=False)
    # if not refine_results.empty:
    #     refine_results.to_csv(f"SOPT_refine_{tag}.csv", index=False)

    # 保存最优参数为可直接 copy-paste 的格式
    with open(f"SOPT_best_{tag}.txt", 'w') as f:
        f.write("# Quant Master V7.2 - 最优策略参数\n")
        f.write(f"# 优化区间: {BACKTEST_START} ~ {BACKTEST_END}\n")
        f.write(f"# 固定退出: TP1={FIXED_TP1} TP2={FIXED_TP2} SL={FIXED_SL} Hold={FIXED_HOLD}\n\n")
        f.write(f"EXPLOSION_LOOKBACK_DAYS = {int(final['lookback'])}\n")
        f.write(f"EXPLOSION_PCT_MIN  = {final['pct_min']}\n")
        f.write(f"EXPLOSION_PCT_MAX  = {final['pct_max']}\n")
        f.write(f"EXPLOSION_VOL_MULT = {final['vol_mult']}\n")
        f.write(f"MAX_DIST_MA10      = {final['ma10_high']}\n")
        f.write(f"MAX_DIST_MA10_LOW  = {final['ma10_low']}\n")
        f.write(f"MAX_DIST_MA10_MA20 = {final['ma10_ma20_dist']}\n")
        f.write(f"MAX_20_DAYS_RISE   = {final['max_20d_rise']}\n")
        f.write(f"MA20_MA30_TOLERANCE  = {final['ma20_30_tol']}\n")
        f.write(f"MA10_SLOPE_TOLERANCE = {final['ma10_slope_tol']}\n")
        f.write(f"MA30_SUPPORT_TOLERANCE = {final['ma30_support']}\n")
        f.write(f"BULL_DAYS_THRESHOLD  = {int(final['bull_threshold'])}\n")
        f.write(f"BULL_DAYS_WINDOW     = {int(final['bull_window'])}\n")
        if final_m:
            f.write(f"\n# 绩效: 胜率={final_m['win_rate']*100:.1f}%"
                    f" 期望={final_m['expectancy']*100:+.2f}%"
                    f" 累计={final_m['total_ret']*100:+.1f}%"
                    f" 笔数={int(final_m['n_trades'])}\n")

    print(f"\n  💾 结果已保存: SOPT_R1/R2/R3_*.csv + SOPT_best_*.txt")
    print(f"\n  🏁 策略优化完成。")