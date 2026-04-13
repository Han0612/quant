import tushare as ts
import pandas as pd
import numpy as np
import datetime
import time
import argparse
import itertools
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# ⚙️  默认配置区
# ============================================================
SHOW_DETAIL = True

# 初始预测参数 (这套参数现在可以作为“种子”，让优化器去进化)
DEFAULT_PARAMS = {
    'momentum_weights': [1, 1, 1.1, 1.1, 1.2],
    'reversion_high_threshold': 65,
    'reversion_high_coef': 0.15,
    'reversion_low_threshold': -50,
    'reversion_low_coef': 0.12,
    'chain_bonus_threshold': 5,
    'chain_bonus_score': 2,
    'chain_penalty_score': -1,
    'explode_high_threshold': 0.60,
    'explode_high_penalty': -2,
    'explode_low_threshold': 0.15,
    'explode_low_bonus': 1,
    'holiday_discount': {1: 1.0, 3: 1.0, 5: 0.85, 99: 0.7}
}

# ============================================================
# 🗓️  交易日历工具类
# ============================================================

class TradingCalendar:
    def __init__(self, pro):
        self.pro = pro
        self._cache = {}

    def _fetch_calendar(self, year: int) -> pd.DataFrame:
        if year in self._cache: return self._cache[year]
        df = self.pro.trade_cal(exchange='SSE', start_date=f"{year}0101", end_date=f"{year}1231")
        self._cache[year] = df
        return df

    def get_open_days(self, start_date: str, end_date: str) -> list:
        years = range(int(start_date[:4]), int(end_date[:4]) + 1)
        frames = [self._fetch_calendar(y) for y in years]
        df = pd.concat(frames)
        df = df[(df['cal_date'] >= start_date) & (df['cal_date'] <= end_date)]
        return df[df['is_open'] == 1]['cal_date'].sort_values().tolist()

    def is_trading_day(self, date_str: str) -> bool:
        year = int(date_str[:4]); df = self._fetch_calendar(year)
        row = df[df['cal_date'] == date_str]
        return (not row.empty) and (row.iloc[0]['is_open'] == 1)

    def prev_trading_day(self, date_str: str, n: int = 1) -> str:
        year = int(date_str[:4]); days = self.get_open_days(f"{year - 1}0101", date_str)
        days = [d for d in days if d < date_str]
        return days[-n] if len(days) >= n else days[0]

    def next_trading_day(self, date_str: str) -> str:
        dt = datetime.datetime.strptime(date_str, '%Y%m%d')
        for i in range(1, 31):
            nxt = (dt + datetime.timedelta(days=i)).strftime('%Y%m%d')
            if self.is_trading_day(nxt): return nxt
        return date_str

    def get_latest_trading_day(self) -> str:
        now = datetime.datetime.now(); today_str = now.strftime('%Y%m%d')
        if self.is_trading_day(today_str) and now.hour >= 15: return today_str
        return self.prev_trading_day(today_str, 1)

    def days_until_next_trading(self, date_str: str) -> int:
        nxt = self.next_trading_day(date_str)
        return (datetime.datetime.strptime(nxt, '%Y%m%d') - datetime.datetime.strptime(date_str, '%Y%m%d')).days

# ============================================================
# 📊  数据获取模块
# ============================================================

class DataFetcher:
    def __init__(self, pro, calendar):
        self.pro = pro; self.cal = calendar

    def _safe_call(self, func, *args, **kwargs):
        for i in range(3):
            try:
                res = func(*args, **kwargs)
                if res is not None and not res.empty: return res
            except: time.sleep(0.5)
        return pd.DataFrame()

    def get_limit_list(self, trade_date: str):
        df = self._safe_call(self.pro.limit_list_d, trade_date=trade_date)
        if not df.empty and 'limit' in df.columns: df = df.rename(columns={'limit': 'limit_type'})
        return df

    def get_daily_all(self, trade_date: str):
        return self._safe_call(self.pro.daily, trade_date=trade_date)

    def get_index_daily(self, ts_code: str, trade_date: str):
        df = self._safe_call(self.pro.index_daily, ts_code=ts_code, trade_date=trade_date)
        return df.iloc[0] if not df.empty else pd.Series()

    def get_north_flow(self, trade_date: str):
        df = self._safe_call(self.pro.moneyflow_hsgt, start_date=trade_date, end_date=trade_date)
        return float(df.iloc[0].get('north_money', 0)) if not df.empty else 0

    def get_moneyflow_summary(self, trade_date: str):
        df = self._safe_call(self.pro.moneyflow, trade_date=trade_date)
        if df.empty: return {}
        return {'net_total_yi': df['net_mf_amount'].sum() / 10000, 'inflow_cnt': (df['net_mf_amount'] > 0).sum()}

    def get_lite_snapshot(self, trade_date: str):
        """轻量化快照，回测加速核心"""
        df_l = self.get_limit_list(trade_date); df_d = self.get_daily_all(trade_date)
        b_score = 0; max_s = 0
        if not df_d.empty:
            up_r = (df_d['pct_chg'] > 0).sum() / len(df_d)
            b_score = 25 if up_r >= 0.7 else 15 if up_r >= 0.58 else 5 if up_r >= 0.48 else -10 if up_r >= 0.4 else -25
        if not df_l.empty:
            lu = df_l[df_l['limit_type'] == 'U']
            max_s = int(lu['lb_days'].max()) if not lu.empty and 'lb_days' in lu.columns else 0
        return {'lite_score': b_score, 'max_streak': max_s, 'date': trade_date}

# ============================================================
# 🧠  情绪计算与预测
# ============================================================

class SentimentCalculator:
    def __init__(self, trade_date, fetcher):
        self.trade_date = trade_date; self.fetcher = fetcher; self.scores = {}; self.raw = {}; self.details = []

    def calculate(self):
        df_d = self.fetcher.get_daily_all(self.trade_date); df_l = self.fetcher.get_limit_list(self.trade_date)
        # 简化计算逻辑
        up_r = (df_d['pct_chg'] > 0).sum() / len(df_d) if not df_d.empty else 0
        self.scores['breadth'] = 25 if up_r >= 0.7 else 15 if up_r >= 0.58 else 5 if up_r >= 0.48 else -25
        lu_c = len(df_l[df_l['limit_type'] == 'U']) if not df_l.empty else 0
        self.scores['limit'] = 15 if lu_c >= 80 else 5 if lu_c >= 30 else -10
        total = sum(self.scores.values())
        return {'total_score': total, 'raw': {'max_streak': 0, 'explode_rate': 0.3}, 'date': self.trade_date}

class SentimentPredictor:
    def __init__(self, fetcher, calendar, params):
        self.fetcher = fetcher; self.calendar = calendar; self.params = params

    def predict(self, current_date, current_score, current_raw):
        p = self.params; hist = []
        prev = current_date
        for _ in range(4):
            prev = self.calendar.prev_trading_day(prev); hist.append(self.fetcher.get_lite_snapshot(prev))
        hist.reverse()
        score_seq = [h['lite_score'] for h in hist] + [current_score]
        weights = p['momentum_weights'][-len(score_seq):]
        momentum = sum(s * w for s, w in zip(score_seq, weights)) / sum(weights)
        reversion = 0
        if current_score > p['reversion_high_threshold']:
            reversion = -(current_score - p['reversion_high_threshold']) * p['reversion_high_coef']
        elif current_score < p['reversion_low_threshold']:
            reversion = (p['reversion_low_threshold'] - current_score) * p['reversion_low_coef']
        
        pred_score = int(momentum + reversion)
        return {'predicted_score': pred_score, 'next_date': self.calendar.next_trading_day(current_date)}

# ============================================================
# 📐  优化与回测引擎
# ============================================================

class QuantOptimizer:
    def __init__(self, fetcher, calendar):
        self.fetcher = fetcher; self.calendar = calendar

    def run_backtest(self, test_days, params):
        """核心回测评估函数"""
        today = self.calendar.get_latest_trading_day()
        days = self.calendar.get_open_days(self.calendar.prev_trading_day(today, test_days + 5), today)
        eval_days = days[-(test_days+1):-1]; errors = []; direction_hits = 0
        predictor = SentimentPredictor(self.fetcher, self.calendar, params)
        
        for d in eval_days:
            curr_snap = self.fetcher.get_lite_snapshot(d)
            pred = predictor.predict(d, curr_snap['lite_score'], {'max_streak': curr_snap['max_streak']})
            real_next = self.fetcher.get_lite_snapshot(pred['next_date'])['lite_score']
            errors.append(abs(pred['predicted_score'] - real_next))
            if (pred['predicted_score'] >= 0) == (real_next >= 0): direction_hits += 1
        
        mae = np.mean(errors); acc = direction_hits / len(eval_days)
        return mae, acc

    def optimize(self, days=20):
        print(f"🧬 [Quant Master] 进化引擎启动，正在搜索最优 DNA...")
        # 定义搜索网格
        momentum_space = [[1,1,1,1,1], [1,1,1.1,1.2,1.5], [1,1.5,2,3,5]]
        reversion_space = [0.05, 0.15, 0.30]
        bonus_space = [0, 2, 5]
        
        best_mae = 999; best_acc = 0; best_params = None

        search_combinations = list(itertools.product(momentum_space, reversion_space, bonus_space))
        
        for m_w, r_c, b_s in search_combinations:
            p = DEFAULT_PARAMS.copy()
            p.update({'momentum_weights': m_w, 'reversion_high_coef': r_c, 'chain_bonus_score': b_s})
            mae, acc = self.run_backtest(days, p)
            
            # 综合评分：准确率优先，MAE 辅助
            if acc > best_acc or (acc == best_acc and mae < best_mae):
                best_acc = acc; best_mae = mae; best_params = p
                print(f"✨ 发现更优基因 -> 准确率: {acc:.1%}, MAE: {mae:.1f}")

        print("\n" + "="*50); print("🏆 进化完成！推荐黄金参数组合：")
        print(f"  · momentum_weights: {best_params['momentum_weights']}")
        print(f"  · reversion_high_coef: {best_params['reversion_high_coef']}")
        print(f"  · chain_bonus_score: {best_params['chain_bonus_score']}")
        print("="*50)
        return best_params

# ============================================================
# 🚀  主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, default=None)
    parser.add_argument('--backtest', action='store_true')
    parser.add_argument('--optimize', action='store_true')
    args = parser.parse_args()

    pro = ts.pro_api(); pro._DataApi__token = '4502048184048033923'
    pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'
    
    cal = TradingCalendar(pro); fetcher = DataFetcher(pro, cal)
    
    if args.optimize:
        optimizer = QuantOptimizer(fetcher, cal)
        optimizer.optimize(days=30)
        return

    # 默认运行逻辑...
    target_date = args.date if args.date else cal.get_latest_trading_day()
    print(f"📊 正在执行 {target_date} 情绪分析...")
    # (此处省略与 v1.1 相同的打印逻辑以保持简洁)

if __name__ == "__main__":
    main()