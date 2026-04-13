"""
╔══════════════════════════════════════════════════════════════╗
║       A股市场情绪分析系统 v1.3 (ST_CLIENT 专享版)            ║
║       当日情绪描述 + 次交易日情绪预测                        ║
║       盘中/收盘 自适应双通道                                 ║
║       基于自定义 API 网关全面重构                            ║
╚══════════════════════════════════════════════════════════════╝
"""

import tushare as ts  # 仅保留用于无Token限制的 ts.get_realtime_quotes
import pandas as pd
import numpy as np
import datetime
import time
import argparse
import warnings
warnings.filterwarnings('ignore')

# 引入自定义的 API 客户端
try:
    from ST_CLIENT import StockToday
except ImportError:
    print("❌ 未找到 ST_CLIENT.py，请确保该文件与当前脚本在同一目录下。")
    exit()

# ============================================================
# ⚙️  配置区
# ============================================================
SHOW_DETAIL = True

PREDICTION_PARAMS = {
    'momentum_weights': [1, 1.5, 2, 3, 5],
    'reversion_high_threshold': 50,
    'reversion_high_coef':      0.05,
    'reversion_low_threshold': -35,
    'reversion_low_coef':       0.25,
    'chain_bonus_threshold': 6,
    'chain_bonus_score':     0,
    'chain_penalty_score':  -1,
    'explode_high_threshold': 0.7,
    'explode_high_penalty':  -1,
    'explode_low_threshold':  0.10,
    'explode_low_bonus':       0,
    'holiday_discount': {1: 1.00, 3: 1.00, 5: 0.85, 99: 0.70},
}


# ============================================================
# 🗓️  交易日历
# ============================================================

class TradingCalendar:
    def __init__(self, pro: StockToday):
        self.pro    = pro
        self._cache = {}

    def _fetch_calendar(self, year: int) -> pd.DataFrame:
        if year in self._cache:
            return self._cache[year]
        
        # 使用 ST_CLIENT 请求数据
        res = self.pro.trade_cal(exchange='SSE', start_date=f"{year}0101", end_date=f"{year}1231")
        
        # [核心改造] 将 API 返回的 list 转换为 DataFrame
        df = pd.DataFrame(res) if isinstance(res, list) else pd.DataFrame()
        
        self._cache[year] = df
        return df

    def get_open_days(self, start_date: str, end_date: str) -> list:
        years  = range(int(start_date[:4]), int(end_date[:4]) + 1)
        frames = [self._fetch_calendar(y) for y in years]
        if not frames or all(f.empty for f in frames):
            return []
        df = pd.concat(frames)
        df = df[(df['cal_date'] >= start_date) & (df['cal_date'] <= end_date)]
        return df[df['is_open'] == 1]['cal_date'].sort_values().tolist()

    def is_trading_day(self, date_str: str) -> bool:
        year = int(date_str[:4])
        df   = self._fetch_calendar(year)
        if df.empty: return False
        row  = df[df['cal_date'] == date_str]
        return (not row.empty) and (row.iloc[0]['is_open'] == 1)

    def prev_trading_day(self, date_str: str, n: int = 1) -> str:
        year = int(date_str[:4])
        days = self.get_open_days(f"{year - 1}0101", date_str)
        days = [d for d in days if d < date_str]
        if len(days) < n:
            raise ValueError(f"无法向前回溯 {n} 个交易日")
        return days[-n]

    def next_trading_day(self, date_str: str) -> str:
        dt = datetime.datetime.strptime(date_str, '%Y%m%d')
        for i in range(1, 31):
            nxt = (dt + datetime.timedelta(days=i)).strftime('%Y%m%d')
            if self.is_trading_day(nxt):
                return nxt
        raise ValueError("无法找到下一个交易日")

    def get_latest_trading_day(self) -> str:
        now       = datetime.datetime.now()
        today_str = now.strftime('%Y%m%d')

        if self.is_trading_day(today_str):
            if now.hour > 9 or (now.hour == 9 and now.minute >= 25):
                return today_str
            return self.prev_trading_day(today_str, 1)

        return self.prev_trading_day(today_str, 1)

    def days_until_next_trading(self, date_str: str) -> int:
        nxt = self.next_trading_day(date_str)
        d0  = datetime.datetime.strptime(date_str, '%Y%m%d')
        d1  = datetime.datetime.strptime(nxt, '%Y%m%d')
        return (d1 - d0).days


# ============================================================
# 📊  数据获取模块 (双通道: 实时 + ST_CLIENT)
# ============================================================

class DataFetcher:
    def __init__(self, pro: StockToday, calendar: TradingCalendar):
        self.pro = pro
        self.cal = calendar

    def _safe_call(self, func, *args, retries=3, sleep=0.3, **kwargs):
        """核心封装：自动处理 ST_CLIENT 的返回格式并重试"""
        for i in range(retries):
            try:
                result = func(*args, **kwargs)
                
                # [核心改造] 适配 ST_CLIENT 返回类型
                if isinstance(result, list):
                    df = pd.DataFrame(result)
                elif isinstance(result, pd.DataFrame):
                    df = result
                else:
                    df = pd.DataFrame()

                if not df.empty:
                    return df
            except Exception:
                pass
            time.sleep(sleep * (i + 1))
        return pd.DataFrame()

    @staticmethod
    def is_intraday(trade_date: str) -> bool:
        now       = datetime.datetime.now()
        today_str = now.strftime('%Y%m%d')
        if trade_date != today_str:
            return False
        t = now.hour * 60 + now.minute
        return t >= (9 * 60 + 25) and t < (15 * 60 + 5)

    @staticmethod
    def is_after_close(trade_date: str) -> bool:
        now       = datetime.datetime.now()
        today_str = now.strftime('%Y%m%d')
        if trade_date != today_str:
            return True
        return now.hour >= 16

    def get_realtime_all(self) -> pd.DataFrame:
        """
        盘中通道: 依然保留 ts.get_realtime_quotes() 获取全市场实时行情
        (由于不需要Token，速度最快且最适合盘中)
        """
        print("  📡 正在拉取实时行情 (Crawler Mode)...")

        stock_list = self._safe_call(self.pro.stock_basic, exchange='',
                                      list_status='L', fields='symbol,ts_code')
        if stock_list.empty:
            return pd.DataFrame()

        stock_list = stock_list[stock_list['symbol'].str.startswith(('60', '00'))]
        symbols = stock_list['symbol'].tolist()

        all_dfs = []
        batch_size = 100
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            for attempt in range(3):
                try:
                    time.sleep(0.1)
                    df_batch = ts.get_realtime_quotes(batch)
                    if df_batch is not None and not df_batch.empty and 'code' in df_batch.columns:
                        all_dfs.append(df_batch)
                        break
                except:
                    time.sleep(0.5)

        if not all_dfs:
            return pd.DataFrame()

        df_rt = pd.concat(all_dfs, ignore_index=True)

        df_rt['code'] = df_rt['code'].astype(str).str.strip()
        stock_list['symbol'] = stock_list['symbol'].astype(str).str.strip()
        df_rt = pd.merge(df_rt, stock_list, left_on='code', right_on='symbol', how='inner')

        for c in ['price', 'high', 'low', 'open', 'pre_close', 'volume']:
            if c in df_rt.columns:
                df_rt[c] = pd.to_numeric(df_rt[c], errors='coerce').fillna(0)

        df_rt = df_rt[(df_rt['price'] > 0) & (df_rt['volume'] > 0)].copy()

        df_rt['pre_close'] = df_rt['pre_close'].replace(0, np.nan)
        df_rt['pct_chg'] = ((df_rt['price'] - df_rt['pre_close'])
                            / df_rt['pre_close'] * 100)
        df_rt['pct_chg'] = df_rt['pct_chg'].fillna(0)

        df_rt = df_rt.rename(columns={'price': 'close'})

        n_stocks = len(df_rt)
        print(f"  📡 实时行情: 已获取 {n_stocks} 只主板股票")
        return df_rt

    def estimate_limits_from_realtime(self, df_rt: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        if df_rt.empty: return pd.DataFrame()

        df = df_rt.copy()
        df['pct_chg'] = pd.to_numeric(df['pct_chg'], errors='coerce').fillna(0)
        results = []

        lu_mask = (df['pct_chg'] >= 9.8) & (df['close'] >= df['high'] * 0.999)
        for _, r in df[lu_mask].iterrows():
            results.append({
                'ts_code': r['ts_code'], 'trade_date': trade_date,
                'limit_type': 'U', 'lb_days': 1, 'open_times': 0, 'pct_chg': r['pct_chg'],
            })

        pre = pd.to_numeric(df['pre_close'], errors='coerce').fillna(0)
        high_pct = (df['high'] - pre) / pre.replace(0, np.nan) * 100
        zhaban_mask = (high_pct >= 9.8) & ~lu_mask & (df['pct_chg'] > 0)
        for _, r in df[zhaban_mask].iterrows():
            results.append({
                'ts_code': r['ts_code'], 'trade_date': trade_date,
                'limit_type': 'U', 'lb_days': 1, 'open_times': 1, 'pct_chg': r['pct_chg'],
            })

        ld_mask = (df['pct_chg'] <= -9.8) & (df['close'] <= df['low'] * 1.001)
        for _, r in df[ld_mask].iterrows():
            results.append({
                'ts_code': r['ts_code'], 'trade_date': trade_date,
                'limit_type': 'D', 'lb_days': 1, 'open_times': 0, 'pct_chg': r['pct_chg'],
            })

        if not results: return pd.DataFrame()
        df_limits = pd.DataFrame(results)

        try:
            prev_date = self.cal.prev_trading_day(trade_date)
            df_prev_limit = self._safe_call(self.pro.limit_list_d, trade_date=prev_date)
            if not df_prev_limit.empty:
                if 'limit' in df_prev_limit.columns and 'limit_type' not in df_prev_limit.columns:
                    df_prev_limit = df_prev_limit.rename(columns={'limit': 'limit_type'})
                prev_lu = df_prev_limit[df_prev_limit['limit_type'] == 'U']
                if 'lb_days' in prev_lu.columns:
                    prev_streak = dict(zip(prev_lu['ts_code'], prev_lu['lb_days'].fillna(1).astype(int)))
                    for idx, row in df_limits.iterrows():
                        if row['limit_type'] == 'U' and row['ts_code'] in prev_streak:
                            df_limits.at[idx, 'lb_days'] = prev_streak[row['ts_code']] + 1
        except:
            pass 

        return df_limits

    def get_limit_list(self, trade_date: str) -> pd.DataFrame:
        df = self._safe_call(self.pro.limit_list_d, trade_date=trade_date)
        if df.empty: return df
        if 'limit' in df.columns and 'limit_type' not in df.columns:
            df = df.rename(columns={'limit': 'limit_type'})
        return df

    def get_daily_all(self, trade_date: str) -> pd.DataFrame:
        return self._safe_call(
            self.pro.daily, trade_date=trade_date,
            fields='ts_code,trade_date,open,high,low,close,pre_close,vol,amount,pct_chg'
        )

    def get_index_daily(self, ts_code: str, trade_date: str) -> pd.Series:
        df = self._safe_call(
            self.pro.index_daily, ts_code=ts_code, trade_date=trade_date,
            fields='ts_code,trade_date,pct_chg,vol,amount'
        )
        return df.iloc[0] if not df.empty else pd.Series()

    def get_index_realtime(self, index_codes: list) -> dict:
        result = {}
        for code in index_codes:
            try:
                if '.SH' in code: rt_code = 'sh' + code.split('.')[0]
                elif '.SZ' in code: rt_code = 'sz' + code.split('.')[0]
                else: continue
                df = ts.get_realtime_quotes(rt_code)
                if df is not None and not df.empty:
                    price = float(df.iloc[0].get('price', 0))
                    pre   = float(df.iloc[0].get('pre_close', 0))
                    if pre > 0:
                        result[code] = round((price - pre) / pre * 100, 2)
            except:
                pass
        return result

    def get_north_flow(self, trade_date: str):
        df = self._safe_call(
            self.pro.moneyflow_hsgt, start_date=trade_date, end_date=trade_date
        )
        if df.empty: return None
        return float(df.iloc[0].get('north_money', 0))

    def get_moneyflow_summary(self, trade_date: str) -> dict:
        df = self._safe_call(
            self.pro.moneyflow, trade_date=trade_date,
            fields='ts_code,net_mf_amount,buy_lg_amount,sell_lg_amount'
        )
        if df.empty: return {}
        # 兼容不同列名格式
        if 'net_mf_amount' in df.columns:
            net_total = df['net_mf_amount'].sum() / 10000
            inflow_cnt  = (df['net_mf_amount'] > 0).sum()
            outflow_cnt = (df['net_mf_amount'] < 0).sum()
            return {'net_total_yi': net_total, 'inflow_cnt': inflow_cnt, 'outflow_cnt': outflow_cnt}
        return {}

    def get_lite_snapshot(self, trade_date: str) -> dict:
        df_limit = self.get_limit_list(trade_date)
        df_daily = self.get_daily_all(trade_date)
        time.sleep(0.15)

        breadth_score = 0
        if not df_daily.empty:
            df_daily['pct_chg'] = pd.to_numeric(df_daily['pct_chg'], errors='coerce').fillna(0)
            up_ratio = (df_daily['pct_chg'] > 0).sum() / max(1, len(df_daily))
            if up_ratio >= 0.70:   breadth_score = 25
            elif up_ratio >= 0.58: breadth_score = 15
            elif up_ratio >= 0.48: breadth_score = 5
            elif up_ratio >= 0.40: breadth_score = -10
            else:                  breadth_score = -25

        limit_score = 0
        max_streak  = 0
        if not df_limit.empty:
            lu = df_limit[df_limit['limit_type'] == 'U']
            ld = df_limit[df_limit['limit_type'] == 'D']
            lu_count = len(lu)
            ld_count = len(ld)
            ratio    = lu_count / max(1, lu_count + ld_count)
            s1 = 15 if lu_count >= 100 else (10 if lu_count >= 60 else
                  5 if lu_count >= 30 else  0 if lu_count >= 10 else -10)
            s2 = 10 if ratio >= 0.90 else (5 if ratio >= 0.75 else
                  0 if ratio >= 0.55 else -5 if ratio >= 0.35 else -15)
            limit_score = s1 + s2
            if 'lb_days' in lu.columns and not lu.empty:
                max_streak = int(lu['lb_days'].fillna(1).max())

        lite_score = int(max(-100, min(100, breadth_score + limit_score)))
        return {'date': trade_date, 'lite_score': lite_score, 'max_streak': max_streak}

# ============================================================
# 🧠  当日情绪计算引擎 (逻辑保持不变)
# ============================================================

class SentimentCalculator:
    def __init__(self, trade_date: str, fetcher: DataFetcher):
        self.trade_date = trade_date
        self.fetcher    = fetcher
        self.raw        = {}
        self.scores     = {}
        self.details    = []

    def _score_breadth(self, df_daily):
        total = len(df_daily)
        if total == 0:
            self.scores['breadth'] = 0
            self.details.append("  📊 广度指标 [0分]: 无数据")
            return 0
        df_daily['pct_chg'] = pd.to_numeric(df_daily['pct_chg'], errors='coerce').fillna(0)
        up       = (df_daily['pct_chg'] > 0).sum()
        up_ratio = up / total
        self.raw.update({'up_count': int(up), 'total_count': total, 'up_ratio': round(up_ratio, 4)})

        if up_ratio >= 0.70:   s = 25;  tag = f"全面上涨({up_ratio:.0%}上涨)"
        elif up_ratio >= 0.58: s = 15;  tag = f"多头主导({up_ratio:.0%}上涨)"
        elif up_ratio >= 0.48: s = 5;   tag = f"略偏多({up_ratio:.0%}上涨)"
        elif up_ratio >= 0.40: s = -10; tag = f"空头略占优({up_ratio:.0%}上涨)"
        else:                  s = -25; tag = f"全面下跌({up_ratio:.0%}上涨)"
        self.scores['breadth'] = s
        self.details.append(f"  📊 广度指标 [{s:+d}分]: {tag}")
        return s

    def _score_limit_up(self, df_limit):
        lu = df_limit[df_limit['limit_type'] == 'U'] if not df_limit.empty else pd.DataFrame()
        ld = df_limit[df_limit['limit_type'] == 'D'] if not df_limit.empty else pd.DataFrame()
        lu_count = len(lu)
        ld_count = len(ld)
        limit_ratio = lu_count / max(1, lu_count + ld_count)
        self.raw.update({'limit_up': lu_count, 'limit_down': ld_count, 'limit_ratio': round(limit_ratio, 4)})

        if lu_count >= 100:  s1 = 15; tag1 = f"涨停盛宴({lu_count}只)"
        elif lu_count >= 60: s1 = 10; tag1 = f"涨停活跃({lu_count}只)"
        elif lu_count >= 30: s1 = 5;  tag1 = f"涨停一般({lu_count}只)"
        elif lu_count >= 10: s1 = 0;  tag1 = f"涨停稀少({lu_count}只)"
        else:                s1 = -10; tag1 = f"涨停荒漠({lu_count}只)"

        if limit_ratio >= 0.90:   s2 = 10; tag2 = "做多合力极强"
        elif limit_ratio >= 0.75: s2 = 5;  tag2 = "做多占优"
        elif limit_ratio >= 0.55: s2 = 0;  tag2 = "多空均衡"
        elif limit_ratio >= 0.35: s2 = -5; tag2 = "跌停压制"
        else:                     s2 = -15; tag2 = "恐慌跌停"

        s = s1 + s2
        self.scores['limit_up'] = s
        self.details.append(f"  🔴 涨停指标 [{s:+d}分]: {tag1}，{tag2}(涨跌比={limit_ratio:.0%})")
        return s

    def _score_chain(self, df_limit):
        lu = df_limit[df_limit['limit_type'] == 'U'] if not df_limit.empty else pd.DataFrame()
        if lu.empty or 'lb_days' not in lu.columns:
            self.scores['chain'] = 0
            self.details.append("  🪜 连板梯队 [0分]: 数据缺失")
            return 0

        lu = lu.copy()
        lu['lb_days']  = lu['lb_days'].fillna(1).astype(int)
        max_streak     = int(lu['lb_days'].max())
        two_plus       = int((lu['lb_days'] >= 2).sum())
        three_plus     = int((lu['lb_days'] >= 3).sum())
        self.raw.update({'max_streak': max_streak, 'two_plus_count': two_plus, 'three_plus_count': three_plus})

        if max_streak >= 8:   s1 = 12; tag1 = f"龙头高耸({max_streak}连板)"
        elif max_streak >= 5: s1 = 8;  tag1 = f"强龙在场({max_streak}连板)"
        elif max_streak >= 3: s1 = 4;  tag1 = f"梯队成型({max_streak}连板)"
        elif max_streak >= 2: s1 = 2;  tag1 = f"短梯起步({max_streak}连板)"
        else:                 s1 = -5; tag1 = "无连板/梯队崩塌"

        if two_plus >= 15:   s2 = 8;  tag2 = f"梯队壮观({two_plus}只2板+)"
        elif two_plus >= 8:  s2 = 5;  tag2 = f"梯队健康({two_plus}只2板+)"
        elif two_plus >= 3:  s2 = 2;  tag2 = "梯队薄弱"
        else:                s2 = -5; tag2 = "梯队断层"

        s = s1 + s2
        self.scores['chain'] = s
        self.details.append(f"  🪜 连板梯队 [{s:+d}分]: {tag1}，{tag2}")
        return s

    def _score_explode_rate(self, df_limit):
        lu = df_limit[df_limit['limit_type'] == 'U'] if not df_limit.empty else pd.DataFrame()
        if lu.empty or 'open_times' not in lu.columns:
            self.scores['explode_rate'] = 0
            self.details.append("  💥 炸板率 [0分]: 数据缺失")
            return 0

        zhaban = (lu['open_times'] > 0).sum()
        total  = len(lu)
        rate   = zhaban / max(1, total)
        self.raw.update({'explode_rate': round(rate, 4), 'zhaban_count': int(zhaban)})

        if rate <= 0.15:   s = 15;  tag = f"封板如铁({rate:.0%}炸板率)"
        elif rate <= 0.25: s = 10;  tag = f"封板稳固({rate:.0%}炸板率)"
        elif rate <= 0.40: s = 3;   tag = f"炸板一般({rate:.0%}炸板率)"
        elif rate <= 0.55: s = -5;  tag = f"炸板偏多({rate:.0%}炸板率)"
        else:              s = -15; tag = f"炸板严重({rate:.0%}炸板率)"

        self.scores['explode_rate'] = s
        self.details.append(f"  💥 炸板情况 [{s:+d}分]: {tag}")
        return s

    def _score_north_flow(self, north_net):
        if north_net is None:
            self.scores['north_flow'] = 0
            self.details.append("  🌐 北向资金 [0分]: 数据暂缺")
            return 0
        self.raw['north_net_yi'] = round(north_net, 2)
        if north_net >= 100:   s = 10;  tag = f"爆买({north_net:.1f}亿)"
        elif north_net >= 30:  s = 7;   tag = f"净买入({north_net:.1f}亿)"
        elif north_net >= 0:   s = 3;   tag = f"小幅买入({north_net:.1f}亿)"
        elif north_net >= -30: s = -3;  tag = f"小幅卖出({north_net:.1f}亿)"
        elif north_net >= -80: s = -7;  tag = f"净卖出({north_net:.1f}亿)"
        else:                  s = -10; tag = f"大幅出逃({north_net:.1f}亿)"
        self.scores['north_flow'] = s
        self.details.append(f"  🌐 北向资金 [{s:+d}分]: {tag}")
        return s

    def _score_moneyflow(self, mf):
        if not mf:
            self.scores['moneyflow'] = 0
            self.details.append("  💰 主力资金 [0分]: 数据暂缺")
            return 0
        net = mf.get('net_total_yi', 0)
        self.raw.update({'mf_net_yi': round(net, 2), 'mf_inflow_cnt': mf.get('inflow_cnt', 0)})
        if net >= 200:    s = 5;  tag = f"主力爆买({net:.0f}亿净流入)"
        elif net >= 50:   s = 3;  tag = f"主力净买入({net:.0f}亿)"
        elif net >= 0:    s = 1;  tag = f"主力小幅流入({net:.0f}亿)"
        elif net >= -50:  s = -1; tag = f"主力小幅流出({net:.0f}亿)"
        elif net >= -200: s = -3; tag = f"主力净卖出({net:.0f}亿)"
        else:             s = -5; tag = f"主力大幅出货({net:.0f}亿)"
        self.scores['moneyflow'] = s
        self.details.append(f"  💰 主力资金 [{s:+d}分]: {tag}")
        return s

    def calculate(self) -> dict:
        is_intraday = DataFetcher.is_intraday(self.trade_date)
        is_settled  = DataFetcher.is_after_close(self.trade_date)

        if is_intraday:
            mode_tag = "📡 盘中实时模式"
            print(f"\n  {mode_tag}: 使用 get_realtime_quotes() 实时通道")
            df_daily = self.fetcher.get_realtime_all()
            df_limit = self.fetcher.estimate_limits_from_realtime(df_daily, self.trade_date)
            idx_rt = self.fetcher.get_index_realtime(['000300.SH', '000905.SH'])
            self.raw['hs300_pct'] = idx_rt.get('000300.SH')
            self.raw['zz500_pct'] = idx_rt.get('000905.SH')

            prev_date = self.fetcher.cal.prev_trading_day(self.trade_date)
            north_net = self.fetcher.get_north_flow(prev_date)
            mf        = self.fetcher.get_moneyflow_summary(prev_date)

            self.details.append("  📡 [盘中] 涨跌停为实时估算, 炸板率/连板精度约80%")
            self.details.append("  📡 [盘中] 北向资金/主力资金为昨日(T-1)数据")

        elif is_settled:
            mode_tag = "✅ 收盘完整模式"
            print(f"\n  {mode_tag}: 使用 ST_CLIENT 接口请求")

            df_daily = self.fetcher.get_daily_all(self.trade_date)
            df_limit = self.fetcher.get_limit_list(self.trade_date)
            north_net = self.fetcher.get_north_flow(self.trade_date)
            mf        = self.fetcher.get_moneyflow_summary(self.trade_date)

            hs300 = self.fetcher.get_index_daily('000300.SH', self.trade_date)
            zz500 = self.fetcher.get_index_daily('000905.SH', self.trade_date)
            self.raw['hs300_pct'] = round(float(hs300.get('pct_chg', 0)), 2) if not hs300.empty else None
            self.raw['zz500_pct'] = round(float(zz500.get('pct_chg', 0)), 2) if not zz500.empty else None
        else:
            mode_tag = "⏳ 收盘过渡模式 (数据入库中)"
            print(f"\n  {mode_tag}: 先尝试ST_CLIENT接口, 若为空则用实时通道")

            df_daily = self.fetcher.get_daily_all(self.trade_date)
            if df_daily.empty:
                print("  ⏳ API数据尚未入库, 切换实时通道...")
                df_daily = self.fetcher.get_realtime_all()
                df_limit = self.fetcher.estimate_limits_from_realtime(df_daily, self.trade_date)
                self.details.append("  ⏳ 使用实时数据估算, 稍后可重新运行获取精确数据")
            else:
                df_limit = self.fetcher.get_limit_list(self.trade_date)
                if df_limit.empty:
                    df_limit = self.fetcher.estimate_limits_from_realtime(df_daily, self.trade_date)

            north_net = self.fetcher.get_north_flow(self.trade_date)
            if north_net is None:
                prev_date = self.fetcher.cal.prev_trading_day(self.trade_date)
                north_net = self.fetcher.get_north_flow(prev_date)
            mf = self.fetcher.get_moneyflow_summary(self.trade_date)
            if not mf:
                prev_date = self.fetcher.cal.prev_trading_day(self.trade_date)
                mf = self.fetcher.get_moneyflow_summary(prev_date)

            hs300 = self.fetcher.get_index_daily('000300.SH', self.trade_date)
            zz500 = self.fetcher.get_index_daily('000905.SH', self.trade_date)
            self.raw['hs300_pct'] = round(float(hs300.get('pct_chg', 0)), 2) if not hs300.empty else None
            self.raw['zz500_pct'] = round(float(zz500.get('pct_chg', 0)), 2) if not zz500.empty else None

        self.raw['is_intraday'] = is_intraday
        self.raw['mode_tag']    = mode_tag

        self._score_breadth(df_daily)
        self._score_limit_up(df_limit)
        self._score_chain(df_limit)
        self._score_explode_rate(df_limit)
        self._score_north_flow(north_net)
        self._score_moneyflow(mf)

        total = max(-100, min(100, sum(self.scores.values())))

        if total >= 70:    level = "🔥 极度贪婪"; advice = "市场赚钱效应强，妖股策略满仓操作"
        elif total >= 40:  level = "📈 偏多乐观"; advice = "情绪偏暖，可正常执行回调接力策略"
        elif total >= 10:  level = "😐 中性偏多"; advice = "情绪中性，轻仓试探，严控止损"
        elif total >= -20: level = "😶 中性偏空"; advice = "情绪不明，建议观察为主，空仓等待"
        elif total >= -50: level = "📉 偏空悲观"; advice = "情绪较差，策略暂停，等待企稳信号"
        else:              level = "❄️ 极度恐慌"; advice = "全面下跌/踩踏，坚决空仓，切勿抄底"

        return {
            'date': self.trade_date, 'total_score': total,
            'level': level, 'advice': advice, 'mode_tag': mode_tag,
            'scores': self.scores, 'raw': self.raw, 'details': self.details,
        }


# ============================================================
# 🔮  次日情绪预测 & 回测模块 & 报告输出 (逻辑保持不变)
# ============================================================

class SentimentPredictor:
    def __init__(self, fetcher: DataFetcher, calendar: TradingCalendar,
                 params: dict = None):
        self.fetcher  = fetcher
        self.calendar = calendar
        self.params   = params or PREDICTION_PARAMS

    def _get_historical_lite_scores(self, target_date: str, n: int = 4) -> list:
        results   = []
        prev_date = target_date
        for _ in range(n):
            try:
                prev_date = self.calendar.prev_trading_day(prev_date)
            except:
                break
            snap = self.fetcher.get_lite_snapshot(prev_date)
            results.append(snap)
        results.reverse()
        return results

    def _holiday_discount(self, current_date: str) -> tuple:
        gap   = self.calendar.days_until_next_trading(current_date)
        table = self.params['holiday_discount']
        if gap <= 1:
            return table.get(1, 1.0), None
        elif gap <= 3:
            return table.get(3, 1.0), "⏰ 正常周末休市"
        elif gap <= 5:
            f = table.get(5, 0.85)
            return f, f"🏖️ 小长假({gap-1}天)，折扣{int((1-f)*100)}%"
        else:
            f = table.get(99, 0.70)
            return f, f"🎉 长假({gap-1}天)，折扣{int((1-f)*100)}%"

    def predict(self, current_date: str, current_score: int,
                current_raw: dict) -> dict:
        p         = self.params
        next_date = self.calendar.next_trading_day(current_date)
        holiday_factor, holiday_msg = self._holiday_discount(current_date)

        print(f"\n  正在获取轻量历史序列...")
        hist = self._get_historical_lite_scores(current_date, n=4)

        score_seq = [h['lite_score'] for h in hist] + [current_score]
        weights   = p['momentum_weights'][-len(score_seq):]
        momentum  = sum(s * w for s, w in zip(score_seq, weights)) / sum(weights)

        reversion, rev_msg = 0, ""
        if current_score > p['reversion_high_threshold']:
            reversion = -(current_score - p['reversion_high_threshold']) * p['reversion_high_coef']
            rev_msg   = f"（高位回归修正 {reversion:+.1f}）"
        elif current_score < p['reversion_low_threshold']:
            reversion = (p['reversion_low_threshold'] - current_score) * p['reversion_low_coef']
            rev_msg   = f"（恐慌反弹修正 {reversion:+.1f}）"

        raw_predict = momentum + reversion

        max_streak  = current_raw.get('max_streak', 0)
        chain_delta, chain_msg = 0, ""
        if max_streak >= p['chain_bonus_threshold']:
            chain_delta = p['chain_bonus_score']
            chain_msg   = f"（强龙惯性 {chain_delta:+d}）"
        elif max_streak == 0:
            chain_delta = p['chain_penalty_score']
            chain_msg   = f"（无梯队拖累 {chain_delta:+d}）"
        raw_predict += chain_delta

        explode_rate = current_raw.get('explode_rate', 0.5)
        if explode_rate > p['explode_high_threshold']:
            raw_predict += p['explode_high_penalty']
        elif explode_rate < p['explode_low_threshold']:
            raw_predict += p['explode_low_bonus']

        if holiday_factor < 1.0:
            raw_predict *= holiday_factor

        predicted_score = int(max(-100, min(100, raw_predict)))

        if predicted_score >= 70:    pred_level = "🔥 极度贪婪"
        elif predicted_score >= 40:  pred_level = "📈 偏多乐观"
        elif predicted_score >= 10:  pred_level = "😐 中性偏多"
        elif predicted_score >= -20: pred_level = "😶 中性偏空"
        elif predicted_score >= -50: pred_level = "📉 偏空悲观"
        else:                        pred_level = "❄️ 极度恐慌"

        if predicted_score >= 40 and current_score >= 20:
            decision = "✅ 双日情绪共振，次日可正常执行买入策略"
        elif predicted_score >= 20:
            decision = "⚠️ 次日情绪中性，轻仓试探，单笔仓位不超过5%"
        elif predicted_score >= -10:
            decision = "⏸️ 次日情绪偏弱，建议观察，不主动建仓"
        else:
            decision = "🚫 次日情绪差，策略熔断，严禁买入"

        history = [(h['date'], h['lite_score']) for h in hist] + [(current_date, current_score)]

        return {
            'next_date': next_date, 'predicted_score': predicted_score,
            'predicted_level': pred_level, 'decision': decision,
            'momentum_base': round(momentum, 1), 'reversion_note': rev_msg,
            'chain_note': chain_msg, 'holiday_factor': holiday_factor,
            'holiday_note': holiday_msg, 'score_history': history,
        }

class Backtester:
    def __init__(self, fetcher: DataFetcher, calendar: TradingCalendar,
                 params: dict = None):
        self.fetcher   = fetcher
        self.calendar  = calendar
        self.params    = params or PREDICTION_PARAMS
        self.predictor = SentimentPredictor(fetcher, calendar, params)

    def run(self, n_days: int = 30) -> dict:
        today    = self.calendar.get_latest_trading_day()
        all_days = self.calendar.get_open_days(
            self.calendar.prev_trading_day(today, n_days + 10), today)
        eval_days = all_days[-(n_days + 1):-1]

        records = []
        print(f"\n  🔬 回测：{len(eval_days)} 个交易日...")

        for i, d in enumerate(eval_days):
            try:
                snap         = self.fetcher.get_lite_snapshot(d)
                actual_today = snap['lite_score']
                pred         = self.predictor.predict(
                    d, actual_today,
                    {'max_streak': snap['max_streak'], 'explode_rate': 0.3})
                next_d       = pred['next_date']
                predicted    = pred['predicted_score']
                next_snap    = self.fetcher.get_lite_snapshot(next_d)
                actual_next  = next_snap['lite_score']
                error        = predicted - actual_next
                direction_ok = ((predicted >= 0) == (actual_next >= 0))

                records.append({
                    'date': d, 'next_date': next_d,
                    'actual_today': actual_today,
                    'predicted': predicted, 'actual_next': actual_next,
                    'error': error, 'abs_error': abs(error),
                    'direction_ok': direction_ok,
                })
                status = '✅' if direction_ok else '❌'
                print(f"    [{i+1:3d}/{len(eval_days)}] {d}→{next_d}  "
                      f"预测:{predicted:+d}  实际:{actual_next:+d}  "
                      f"误差:{error:+d}  {status}")
                time.sleep(0.2)
            except Exception as e:
                print(f"    [{i+1:3d}/{len(eval_days)}] {d} 跳过 ({e})")
                continue

        if not records:
            print("  ⚠️ 无有效回测数据")
            return {}

        df  = pd.DataFrame(records)
        mae = df['abs_error'].mean()
        rmse = float(np.sqrt((df['error'] ** 2).mean()))
        direction_acc = df['direction_ok'].mean()
        error_dist = {
            '±5分以内':  float((df['abs_error'] <= 5).mean()),
            '±10分以内': float((df['abs_error'] <= 10).mean()),
            '±20分以内': float((df['abs_error'] <= 20).mean()),
            '±20分以上': float((df['abs_error'] > 20).mean()),
        }
        suggestions = []
        if mae > 20:  suggestions.append("⚠️ MAE>20, 降低momentum最新权重")
        if mae <= 15: suggestions.append("✅ MAE≤15, 误差可接受")
        if direction_acc < 0.55: suggestions.append("⚠️ 方向准确率<55%, 降低reversion_coef")
        if direction_acc >= 0.65: suggestions.append("✅ 方向准确率≥65%, 稳健")

        report = {'n_eval': len(records), 'mae': round(mae, 2),
                  'rmse': round(rmse, 2), 'direction_acc': round(direction_acc, 4),
                  'error_dist': error_dist, 'suggestions': suggestions}
        self._print_report(report)
        return report

    def _print_report(self, r):
        W = 70
        print(f"\n{'═'*W}")
        print(f"  📐 回测报告 (样本: {r['n_eval']})")
        print(f"{'─'*W}")
        print(f"  MAE: {r['mae']:.1f}  RMSE: {r['rmse']:.1f}  方向准确率: {r['direction_acc']:.1%}")
        print(f"\n  误差分布:")
        for label, ratio in r['error_dist'].items():
            bar = '█' * int(ratio * 30)
            print(f"    {label:12s}  {ratio:.0%}  {bar}")
        for s in r['suggestions']:
            print(f"  {s}")
        print(f"{'═'*W}\n")

def _fmt_num(val, suffix=''):
    if val is None: return 'N/A'
    try: return f"{val:+g}{suffix}"
    except: return 'N/A'

def print_report(today_report, pred_report):
    W = 70
    print('═' * W)
    print(f"  A 股 市 场 情 绪 分 析 报 告")
    print(f"  分析日期: {today_report['date']}  |  {today_report.get('mode_tag', '')}")
    print('═' * W)

    score = today_report['total_score']
    print(f"\n  情绪等级:  {today_report['level']}")
    print(f"  综合得分:  {score:+d} / 100")
    bar_len = int(abs(score) * 30 / 100)
    if score >= 0:
        print(f"  得分图示:  [ {'░'*(30-bar_len)}{'█'*bar_len} ] +{score}")
    else:
        print(f"  得分图示:  [ {'█'*bar_len}{'░'*(30-bar_len)} ] {score}")
    print(f"\n  操作建议:  {today_report['advice']}")

    raw = today_report['raw']
    print(f"\n{'─'*W}")
    print(f"  📌 关键指标速览")
    print(f"{'─'*W}")
    kv = lambda l, v: print(f"    · {l:<18} {v}")
    kv("沪深300涨跌", _fmt_num(raw.get('hs300_pct'), '%'))
    kv("中证500涨跌", _fmt_num(raw.get('zz500_pct'), '%'))
    up_r = raw.get('up_ratio')
    kv("上涨家数比", f"{up_r:.1%}  ({raw.get('up_count','?')}/{raw.get('total_count','?')})" if up_r else "N/A")
    kv("涨停家数", f"{raw.get('limit_up', 'N/A')}只")
    kv("跌停家数", f"{raw.get('limit_down', 'N/A')}只")
    kv("最高连板数", f"{raw.get('max_streak', 'N/A')}板")
    kv("2板以上家数", f"{raw.get('two_plus_count', 'N/A')}只")
    exp = raw.get('explode_rate')
    kv("炸板率", f"{exp:.0%}  (炸板{raw.get('zhaban_count','?')}次)" if exp is not None else "N/A")
    kv("北向净流入", _fmt_num(raw.get('north_net_yi'), '亿'))
    kv("主力净流入", _fmt_num(raw.get('mf_net_yi'), '亿'))

    if SHOW_DETAIL:
        print(f"\n{'─'*W}")
        print(f"  🔬 各维度得分明细")
        print(f"{'─'*W}")
        for line in today_report['details']:
            print(line)

    print(f"\n{'─'*W}")
    print(f"  🔮 次日({pred_report['next_date']}) 情绪预测")
    print(f"{'─'*W}")
    ps = pred_report['predicted_score']
    print(f"\n  预测情绪:  {pred_report['predicted_level']}")
    print(f"  预测得分:  {ps:+d} / 100")
    print(f"  交易决策:  {pred_report['decision']}")
    print(f"\n  ── 推导过程 ──")
    print(f"    动量基准  : {pred_report['momentum_base']:+.1f}")
    if pred_report['reversion_note']: print(f"    均值回归  : {pred_report['reversion_note']}")
    if pred_report['chain_note']:     print(f"    连板惯性  : {pred_report['chain_note']}")
    if pred_report['holiday_note']:   print(f"    节假日    : {pred_report['holiday_note']}")

    print(f"\n  ── 近期情绪轨迹 ──")
    for d, s in pred_report['score_history']:
        is_today = (d == today_report['date'])
        bar_c    = int(abs(s) * 20 / 100)
        bar_v    = ("+" if s >= 0 else "-") + "█" * bar_c
        arrow    = "◀ 今日" if is_today else ""
        print(f"    {d}  {s:>+5}分  {bar_v:<22} {arrow}")
    print(f"    {'─'*35}")
    print(f"    {pred_report['next_date']}  {ps:>+5}分  (预测)")

    print('\n' + '═' * W)
    print(f"  ⚠️ 免责声明: 仅供参考，不构成投资建议。")
    print('═' * W)


# ============================================================
# 🚀  主程序
# ============================================================

def main(target_date=None, run_backtest=False, backtest_days=30):
    print("\n" + "═" * 70)
    print("  🚀 A股市场情绪分析系统 v1.3 (ST_CLIENT 版)")
    print("═" * 70)

    try:
        # [核心改造] 使用你提供的 StockToday 类初始化 API 客户端
        pro = StockToday()
        print("✅ ST_CLIENT 自定义数据通道已激活。")
    except Exception as e:
        print(f"❌ ST_CLIENT 初始化失败: {e}"); exit()

    calendar = TradingCalendar(pro)
    fetcher  = DataFetcher(pro, calendar)

    if run_backtest:
        print(f"\n  📐 回测模式: {backtest_days} 个交易日")
        Backtester(fetcher, calendar).run(n_days=backtest_days)
        return

    if target_date:
        if not calendar.is_trading_day(target_date):
            print(f"⚠️ {target_date} 非交易日，回退到上一交易日")
            target_date = calendar.prev_trading_day(target_date)
    else:
        target_date = calendar.get_latest_trading_day()

    is_intraday = DataFetcher.is_intraday(target_date)
    mode = "盘中实时" if is_intraday else "收盘完整"
    print(f"  📅 分析日期: {target_date}  模式: {mode}")

    print(f"\n{'─'*70}")
    print(f"  📊 第一阶段: 计算 {target_date} 情绪分...")
    calc         = SentimentCalculator(target_date, fetcher)
    today_report = calc.calculate()
    print(f"  当日情绪分: {today_report['total_score']:+d} ({today_report['level']})")

    print(f"\n{'─'*70}")
    print(f"  🔮 第二阶段: 预测次日情绪...")
    predictor   = SentimentPredictor(fetcher, calendar)
    pred_report = predictor.predict(
        target_date, today_report['total_score'], today_report['raw'])
    print(f"  次日预测分: {pred_report['predicted_score']:+d} ({pred_report['predicted_level']})")

    print_report(today_report, pred_report)
    return today_report, pred_report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='A股市场情绪分析系统 v1.3 (ST_CLIENT 版)')
    parser.add_argument('--date', type=str, default=None)
    parser.add_argument('--backtest', action='store_true')
    parser.add_argument('--backtest-days', type=int, default=30)
    args = parser.parse_args()
    main(args.date, args.backtest, args.backtest_days)
