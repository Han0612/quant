import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings

# 忽略pandas的切片警告
warnings.filterwarnings('ignore')

# 【新增】强制禁用系统代理，防止SSL握手错误
os.environ['NO_PROXY'] = 'eastmoney.com'
os.environ['no_proxy'] = 'eastmoney.com'

# 全局计数器
filter_stats = {
    "1_trend_error": 0,      # MA10今日未上行
    "2_multi_head_error": 0, # 【核心】14天内均线未持续多头排列
    "3_ma_dist_error": 0,    # 10/20日线距离太远
    "4_candle_error": 0,     # 不是阴线
    "5_vol_error": 0,        # 没有缩量
    "6_price_pos_error": 0,  # 跌破MA10 或 离MA10太远
    "7_high_pos_error": 0,   # 高位乖离过大
    "8_explode_error": 0     # 无爆量资金介入
}

class TailEndStrategy:
    def __init__(self):
        self.lookback_days = 60  
        
    def get_market_snapshot(self):
        """获取全市场实时快照 (含重试)"""
        max_retries = 5
        for i in range(max_retries):
            try:
                print(f"正在拉取全市场实时行情 (尝试 {i+1}/{max_retries})...")
                df = ak.stock_zh_a_spot_em()
                df = df.rename(columns={
                    '代码': 'symbol', '名称': 'name', '最新价': 'close', 
                    '最高': 'high', '最低': 'low', '今开': 'open', 
                    '昨收': 'pre_close', '成交量': 'volume', '成交额': 'amount',
                    '涨跌幅': 'pct_chg'
                })
                print("行情拉取成功！")
                return df
            except Exception as e:
                print(f"拉取失败: {e}")
                time.sleep(3)
                if i == max_retries - 1: raise e

    def filter_universe(self, df_snapshot):
        """只看上证主板、深证主板"""
        valid_symbols = []
        for code in df_snapshot['symbol']:
            if code.startswith(('60', '00')):
                valid_symbols.append(code)
        
        df_filtered = df_snapshot[df_snapshot['symbol'].isin(valid_symbols)]
        df_filtered = df_filtered[df_filtered['volume'] > 0] 
        return df_filtered

    def check_individual_stock(self, symbol, snapshot_row):
        """核心策略逻辑：修复日期BUG + 14天严格多头"""
        try:
            # 1. 获取历史数据
            df_hist = None
            for retry in range(3):
                try:
                    df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
                    if df_hist is not None and not df_hist.empty: break
                except: time.sleep(0.1)
            
            if df_hist is None or len(df_hist) < 40: 
                return None
            
            # 数据清洗
            df_hist = df_hist[['日期', '开盘', '收盘', '最高', '最低', '成交量', '涨跌幅']]
            df_hist.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'pct_chg']
            
            # --- 【关键修复】日期格式统一处理 ---
            # 必须先转成标准字符串，否则Timestamp和String比较会出错，导致盘后数据重复拼接
            df_hist['date'] = pd.to_datetime(df_hist['date']).dt.strftime('%Y-%m-%d')
            
            last_hist_date = df_hist.iloc[-1]['date']
            current_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
            is_intraday = False
            
            # 只有当历史数据里没有今天时，才拼接
            if last_hist_date != current_date_str:
                is_intraday = True
                new_row = {
                    'date': current_date_str,
                    'open': snapshot_row['open'],
                    'close': snapshot_row['close'],
                    'high': snapshot_row['high'],
                    'low': snapshot_row['low'],
                    'volume': snapshot_row['volume'],
                    'pct_chg': snapshot_row['pct_chg']
                }
                df_hist = pd.concat([df_hist, pd.DataFrame([new_row])], ignore_index=True)
            
            # 双重保险：去重
            df_hist = df_hist.drop_duplicates(subset=['date'], keep='last')

            # 类型转换
            cols = ['close', 'open', 'high', 'low', 'volume', 'pct_chg']
            df_hist[cols] = df_hist[cols].apply(pd.to_numeric, errors='coerce')
            
            # --- 指标计算 ---
            df_hist['MA10'] = df_hist['close'].rolling(window=10).mean()
            df_hist['MA20'] = df_hist['close'].rolling(window=20).mean()
            df_hist['MA30'] = df_hist['close'].rolling(window=30).mean() 
            df_hist['MA60'] = df_hist['close'].rolling(window=60).mean() 
            df_hist['Vol_MA5'] = df_hist['volume'].rolling(window=5).mean()
            
            today = df_hist.iloc[-1]
            yesterday = df_hist.iloc[-2]
            
            global filter_stats

            # ================= 策略逻辑校验 =================
            
            # 条件3 (Part A): MA10今日必须上行
            if not (today['MA10'] > yesterday['MA10']):
                filter_stats["1_trend_error"] += 1
                return None

            # 条件3 (Part B): 【严格多头排列检查】
            # 要求：过去14个交易日（含今天），必须满足 MA10 > MA20 > MA30
            history_14 = df_hist.iloc[-7:] 
            
            # 1. 检查14天内 MA10 是否始终大于 MA20
            check_10_gt_20 = (history_14['MA10'] > history_14['MA20']).all()
            
            # 2. 检查14天内 MA20 是否始终大于 MA30 (确保是标准多头)
            check_20_gt_30 = (history_14['MA20'] > history_14['MA30']).all()
            
            # 3. 检查14天内 收盘价 是否始终大于 MA20 (趋势未破位防守线)
            check_price_safe = (history_14['close'] > history_14['MA20']).all()

            if not (check_10_gt_20 and check_20_gt_30 and check_price_safe):
                filter_stats["2_multi_head_error"] += 1
                return None
            
            # 条件4：10日线接近20日线 (距离近)
            if today['MA20'] == 0: return None
            dist_ma10_ma20 = (today['MA10'] - today['MA20']) / today['MA20']
            if dist_ma10_ma20 > 0.06 or dist_ma10_ma20 < 0: 
                filter_stats["3_ma_dist_error"] += 1
                return None

            # 条件6 (Part A): 当天是阴线
            if not (today['close'] < today['open']):
                filter_stats["4_candle_error"] += 1
                return None

            # 条件6 (Part B): 缩量下跌 (今日量 < 昨日量)
            current_vol = today['volume']
            compare_vol = yesterday['volume']
            
            final_vol_check = current_vol
            if is_intraday:
                now = datetime.datetime.now()
                trade_minutes = 0
                if now.hour < 9 or (now.hour == 9 and now.minute < 30): trade_minutes = 1 
                elif 9 <= now.hour < 11 or (now.hour == 11 and now.minute <= 30):
                    trade_minutes = (now.hour - 9) * 60 + now.minute - 30
                elif 11 < now.hour < 13: trade_minutes = 120 
                elif 13 <= now.hour < 15: trade_minutes = 120 + (now.hour - 13) * 60 + now.minute
                else: trade_minutes = 240 

                if trade_minutes > 0 and trade_minutes < 240:
                    final_vol_check = current_vol * (240 / trade_minutes)
            
            # 缩量检查 (修复了重复数据BUG后，这里可以用严格的小于号)
            if final_vol_check >= compare_vol:
                filter_stats["5_vol_error"] += 1
                return None 

            # 条件2：回调到10日线附近 (5%以内) 且 现价 >= MA10
            if today['MA10'] == 0: return None
            dist_ma10 = (today['close'] - today['MA10']) / today['MA10']
            
            # 1. 必须 Close >= MA10 (dist >= 0)
            # 2. 乖离率 <= 0.05
            if dist_ma10 >= 0.1 or dist_ma10 < 0: 
                filter_stats["6_price_pos_error"] += 1
                return None
            
            # 条件7：非高位 (MA60乖离 < 30%)
            if today['MA60'] > 0:
                bias_60 = (today['close'] - today['MA60']) / today['MA60']
                if bias_60 > 0.30: 
                    filter_stats["7_high_pos_error"] += 1
                    return None
            
            # 条件1：21个交易日内有明显爆量上涨
            window_df = df_hist.iloc[-12:-1]
            found_explosion = False
            for idx, row in window_df.iterrows():
                if 8 <= row['pct_chg'] <= 20:
                    if row['close'] > row['open'] and row['volume'] > (row['Vol_MA5'] * 1.5): 
                         found_explosion = True
                         break
            
            if not found_explosion:
                filter_stats["8_explode_error"] += 1
                return None
            
            # --- 确定市场板块 ---
            market_name = "其他"
            if symbol.startswith("60"):
                market_name = "上海主板"
            elif symbol.startswith("00"):
                market_name = "深圳主板"

            return {
                '代码': symbol,
                '名称': snapshot_row['name'],
                '市场': market_name,
                '现价': today['close'],
                '涨幅': today['pct_chg'],
                'MA10': round(today['MA10'], 2)
            }

        except Exception as e:
            return None

    def run(self):
        try:
            df_snapshot = self.get_market_snapshot()
        except Exception as e:
            print("无法获取行情数据，程序终止。")
            return

        df_target = self.filter_universe(df_snapshot)
        print(f"初步筛选进入计算池的股票数量: {len(df_target)}")
        print("开始多线程策略计算 (请稍候)...")
        
        results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            future_to_stock = {
                executor.submit(self.check_individual_stock, row['symbol'], row): row['symbol'] 
                for _, row in df_target.iterrows()
            }
            
            for future in tqdm(as_completed(future_to_stock), total=len(df_target)):
                res = future.result()
                if res:
                    results.append(res)
        
        print("\n--- 筛选失败原因统计 (调试用) ---")
        print(filter_stats)

        if results:
            df_res = pd.DataFrame(results)
            print("\n========= 尾盘买入策略选股结果 (14天强多头版) =========")
            print(df_res.to_markdown(index=False))
            filename = f"trend_strong_strategy_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
            df_res.to_csv(filename, index=False, encoding='utf_8_sig')
            print(f"\n结果已保存至: {filename}")
        else:
            print("\n今日无满足所有严格条件的股票。")

if __name__ == "__main__":
    strategy = TailEndStrategy()
    strategy.run()