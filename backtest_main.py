import pandas as pd
import numpy as np
import akshare as ak
import datetime
import time
import random
import warnings
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os


# 忽略pandas的切片警告
warnings.filterwarnings('ignore')

# 【新增】强制禁用系统代理，防止SSL握手错误
os.environ['NO_PROXY'] = 'eastmoney.com'
os.environ['no_proxy'] = 'eastmoney.com'

# 引用你原来的策略文件
try:
    from tail_buy_strategy import TailEndStrategy
except ImportError:
    print("错误：未找到 tail_buy_strategy.py，请确保该文件在当前目录下。")
    exit()

warnings.filterwarnings('ignore')

# 调试模式：开启后会打印选股失败的原因，方便排查
DEBUG_MODE = True 

# ==========================================
# 1. 回测专用策略类 (继承并重写核心逻辑)
# ==========================================
class BacktestTailStrategy(TailEndStrategy):
    def __init__(self):
        super().__init__()

    def check_historical_stock(self, symbol, target_date_str, name="未知"):
        """
        针对历史特定日期进行选股
        target_date_str: 格式 '20250101'
        """
        # 随机延迟，防止被封IP
        time.sleep(random.uniform(0.1, 0.4))
        
        try:
            # 1. 获取足够长的历史数据 (为了计算MA60和14天趋势，往前多取150天)
            dt_target = pd.to_datetime(target_date_str)
            start_date = (dt_target - datetime.timedelta(days=150)).strftime("%Y%m%d")
            
            # 获取日线数据
            df_hist = None
            for _ in range(3): # 重试机制
                try:
                    df_hist = ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=target_date_str, adjust="qfq")
                    if df_hist is not None and not df_hist.empty: break
                    time.sleep(1)
                except: time.sleep(1)
            
            if df_hist is None or df_hist.empty: return "No_Data" if DEBUG_MODE else None
            
            # --- 日期对齐检查 (关键) ---
            # 确保最后一行就是 target_date (如果当天停牌，数据会缺失)
            last_date_ak = str(df_hist.iloc[-1]['日期']) 
            target_date_dash = dt_target.strftime("%Y-%m-%d")
            
            if last_date_ak != target_date_dash:
                 return "Date_Mismatch" if DEBUG_MODE else None

            # --- 数据清洗 ---
            df_hist = df_hist[['日期', '开盘', '收盘', '最高', '最低', '成交量', '涨跌幅']]
            df_hist.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'pct_chg']
            cols = ['close', 'open', 'high', 'low', 'volume', 'pct_chg']
            df_hist[cols] = df_hist[cols].apply(pd.to_numeric, errors='coerce')

            # --- 指标计算 ---
            df_hist['MA5'] = df_hist['close'].rolling(window=5).mean()
            df_hist['MA10'] = df_hist['close'].rolling(window=10).mean()
            df_hist['MA20'] = df_hist['close'].rolling(window=20).mean()
            df_hist['MA30'] = df_hist['close'].rolling(window=30).mean()
            df_hist['MA60'] = df_hist['close'].rolling(window=60).mean()
            df_hist['Vol_MA5'] = df_hist['volume'].rolling(window=5).mean()
            
            if len(df_hist) < 40: return "Data_Too_Short" if DEBUG_MODE else None
            
            today = df_hist.iloc[-1]
            yesterday = df_hist.iloc[-2]
            
            # ================= 策略逻辑复刻 (同步你最新的严格版) =================
            
            # 1. 趋势上行
            if not (today['MA10'] > yesterday['MA10']): return "Trend_MA10_Down" if DEBUG_MODE else None

            # 2. 14天严格多头
            history_14 = df_hist.iloc[-14:] 
            if not ((history_14['MA10'] > history_14['MA20']).all() and 
                    (history_14['MA20'] > history_14['MA30']).all() and 
                    (history_14['close'] > history_14['MA20']).all()):
                return "Multi_Head_Fail" if DEBUG_MODE else None
            
            # 3. 均线距离
            if today['MA20'] == 0: return None
            dist_ma10_ma20 = (today['MA10'] - today['MA20']) / today['MA20']
            if dist_ma10_ma20 > 0.08 or dist_ma10_ma20 < 0: return "MA_Dist_Fail" if DEBUG_MODE else None
            
            # 4. 阴线
            if not (today['close'] < today['open']): return "Not_Green_Candle" if DEBUG_MODE else None
            
            # 5. 缩量 (放宽至1.2倍)
            if today['volume'] >= (yesterday['volume'] * 1.2): return "Volume_Fail" if DEBUG_MODE else None
            
            # 6. 回踩位置
            dist_ma10 = (today['close'] - today['MA10']) / today['MA10']
            if dist_ma10 > 0.08 or dist_ma10 < 0: return "Price_Pos_Fail" if DEBUG_MODE else None
            
            # 7. 非高位
            if today['MA60'] > 0:
                bias_60 = (today['close'] - today['MA60']) / today['MA60']
                if bias_60 > 0.30: return "High_Pos_Fail" if DEBUG_MODE else None
                
            # 8. 爆量检查
            window_df = df_hist.iloc[-21:-1]
            found_explosion = False
            for idx, row in window_df.iterrows():
                if 8 <= row['pct_chg'] <= 20:
                    if row['close'] > row['open'] and row['volume'] > (row['Vol_MA5'] * 1.5): 
                         found_explosion = True
                         break
            if not found_explosion: return "Explode_Fail" if DEBUG_MODE else None
            
            return {
                'symbol': symbol,
                'name': name,
                'buy_date': target_date_str,
                'buy_price': today['close'],
                'MA5_T': today['MA5'],   # T日的MA5，用于次日参考
                'MA10_T': today['MA10']  # T日的MA10，用于次日止损参考
            }

        except Exception as e:
            return None

# ==========================================
# 2. 卖出逻辑引擎 (分钟级仿真)
# ==========================================
class SellEngine:
    @staticmethod
    def get_minute_data(symbol, date_str):
        """获取某日的分时数据，格式: 2025-01-02"""
        try:
            # period='1' 代表1分钟数据
            df_min = ak.stock_zh_a_hist_min_em(symbol=symbol, start_date=date_str, end_date=date_str, period='1', adjust='qfq')
            return df_min
        except:
            return None

    @staticmethod
    def simulate_sell(stock_info, sell_date_str):
        symbol = stock_info['symbol']
        buy_price = stock_info['buy_price']
        ref_ma10 = stock_info['MA10_T'] # 使用买入当天的MA10作为参考(也可以用当天的，但为了防未来函数，用T日或者Open计算的比较稳)
        ref_ma5 = stock_info['MA5_T']
        
        # 转换日期格式用于查询 '20250102' -> '2025-01-02'
        query_date_fmt = pd.to_datetime(sell_date_str).strftime("%Y-%m-%d")
        
        # 随机延迟
        time.sleep(random.uniform(0.1, 0.3))
        df_min = SellEngine.get_minute_data(symbol, query_date_fmt + " 09:30:00")
        
        # 如果没有分时数据（停牌或接口限制），降级为按日线开盘价卖出
        if df_min is None or df_min.empty:
            return {'sell_date': sell_date_str, 'sell_price': buy_price, 'return': 0.0, 'reason': '数据缺失(平出)'}
        
        # 数据预处理
        df_min['时间'] = pd.to_datetime(df_min['时间'])
        df_min = df_min.sort_values('时间')
        
        # 计算 VWAP (分时均价线)
        df_min['cumsum_amount'] = df_min['成交额'].cumsum()
        df_min['cumsum_volume'] = df_min['成交量'].cumsum()
        df_min['VWAP'] = df_min['cumsum_amount'] / df_min['cumsum_volume']
        
        open_price = df_min.iloc[0]['开盘']
        sell_price = 0.0
        sell_reason = ""
        
        # ========================================
        # 执行卖出策略 (5条铁律)
        # ========================================
        
        # 【策略4】低开止损：开在 10日线下方 (这里严格用 T日的MA10 做防守)
        if open_price < ref_ma10:
            return {
                'sell_date': sell_date_str, 
                'sell_price': open_price, 
                'return': (open_price - buy_price)/buy_price, 
                'reason': '4_低开破MA10止损'
            }

        # 遍历每一分钟
        high_return_so_far = -999 # 记录当天最高涨幅
        
        for idx, row in df_min.iterrows():
            curr_p = row['收盘']
            time_s = row['时间'].strftime("%H:%M")
            vwap = row['VWAP']
            p_ret = (curr_p - buy_price) / buy_price # 相对买入价的收益率
            
            # 更新最高涨幅
            if p_ret > high_return_so_far:
                high_return_so_far = p_ret

            # 【策略5】深度回踩：跌破 10日线 3个点 且 拉不回分时线 (简化：当前价<VWAP)
            if curr_p < (ref_ma10 * 0.97) and curr_p < vwap:
                sell_price = curr_p
                sell_reason = "5_深度回踩止损"
                break
            
            # 【策略3】弱势表现：10:00 前冲不破 5日线压力位
            if time_s == "10:00":
                if curr_p < ref_ma5:
                    sell_price = curr_p
                    sell_reason = "3_弱势不过MA5"
                    break
                
                # 【策略1】正常高开/平开：10:00左右，冲高1.5%-2% 且不破分时
                if 0.015 <= p_ret <= 0.025 and curr_p >= vwap:
                    sell_price = curr_p
                    sell_reason = "1_10点达标止盈"
                    break

            # 【策略2】强势冲高：早上冲得猛 (>3%)，回头跌破分时线必出
            if high_return_so_far > 0.03: # 假设 >3% 算猛
                if curr_p < vwap:
                    sell_price = curr_p
                    sell_reason = "2_强势回调破分时"
                    break
            
            # 兜底：尾盘 14:55 强制平仓
            if time_s >= "14:55":
                sell_price = curr_p
                sell_reason = "尾盘平仓"
                break
        
        # 如果循环结束还没卖出 (比如一直涨停)，就以收盘价卖出
        if sell_price == 0.0:
            sell_price = df_min.iloc[-1]['收盘']
            sell_reason = "尾盘平仓"

        return {
            'sell_date': sell_date_str, 
            'sell_price': sell_price,
            'return': (sell_price - buy_price)/buy_price, 
            'reason': sell_reason
        }

# ==========================================
# 3. 回测主控循环
# ==========================================
def run_backtest(start_date='20250101', end_date='20260101'):
    print(f"=== 开始回测: {start_date} 至 {end_date} ===")
    print("注意：为了防止接口封锁，并发数设为 4，且增加了随机延迟，请耐心等待...")
    
    # 1. 获取交易日历
    trade_dates_df = ak.tool_trade_date_hist_sina()
    trade_dates = trade_dates_df['trade_date'].astype(str).tolist()
    valid_dates = [d for d in trade_dates if start_date <= d.replace('-','') <= end_date]
    valid_dates.sort()
    
    # 2. 获取股票列表
    print("正在获取全A股列表...")
    stock_info = ak.stock_zh_a_spot_em()
    pool_df = stock_info[stock_info['代码'].str.startswith(('60', '00'))] # 只测主板
    all_symbols = pool_df['代码'].tolist()
    all_names = dict(zip(pool_df['代码'], pool_df['名称']))
    
    # ⚠️【重要】测试模式：为了快速跑通，先随机取 50 只股票验证
    # 验证没问题后，请注释掉下面两行，跑全市场
    import random
    all_symbols = random.sample(all_symbols, 100) 
    print(f"【测试模式】随机抽取 {len(all_symbols)} 只股票进行验证...")

    strategy = BacktestTailStrategy()
    total_trades = []
    
    # 3. 按日循环
    for i, current_date in enumerate(valid_dates[:-1]):
        next_date = valid_dates[i+1] # 次日卖出
        print(f"\n[{current_date}] 选股中... (次日 {next_date} 卖出)")
        
        buy_list = []
        fail_reasons = {}
        
        # 并发选股 (降低并发数到4)
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_symbol = {
                executor.submit(strategy.check_historical_stock, symbol, current_date, all_names.get(symbol, "未知")): symbol 
                for symbol in all_symbols
            }
            
            for future in tqdm(as_completed(future_to_symbol), total=len(all_symbols), leave=False):
                res = future.result()
                if isinstance(res, dict):
                    buy_list.append(res)
                elif isinstance(res, str):
                    fail_reasons[res] = fail_reasons.get(res, 0) + 1
        
        print(f"  -> 选中 {len(buy_list)} 只股票")
        if len(buy_list) == 0 and fail_reasons:
            top_fails = sorted(fail_reasons.items(), key=lambda x: x[1], reverse=True)[:3]
            print(f"     (主要落选原因: {top_fails})")
        
        # 模拟卖出
        for stock in buy_list:
            sell_res = SellEngine.simulate_sell(stock, next_date)
            total_trades.append({
                'buy_date': current_date, 
                'name': stock['name'],
                'buy_price': stock['buy_price'], 
                'sell_price': sell_res['sell_price'],
                'return_pct': round(sell_res['return'] * 100, 2), 
                'reason': sell_res['reason']
            })
            
    # 4. 统计结果
    print("\n\n" + "="*40)
    if not total_trades:
        print("无交易记录。")
        return

    df_res = pd.DataFrame(total_trades)
    win_rate = len(df_res[df_res['return_pct'] > 0]) / len(df_res)
    total_return = df_res['return_pct'].sum()
    avg_return = df_res['return_pct'].mean()
    
    print(f"交易总次数: {len(df_res)}")
    print(f"胜率: {win_rate:.2%}")
    print(f"平均单笔收益: {avg_return:.2f}%")
    print(f"累计收益(单利): {total_return:.2f}%")
    
    print("\n收益 Top 5:")
    print(df_res.sort_values('return_pct', ascending=False).head(5)[['name', 'buy_date', 'return_pct', 'reason']].to_markdown(index=False))
    
    print("\n亏损 Top 5:")
    print(df_res.sort_values('return_pct', ascending=True).head(5)[['name', 'buy_date', 'return_pct', 'reason']].to_markdown(index=False))
    
    filename = f"backtest_report_{start_date}_{end_date}.csv"
    df_res.to_csv(filename, index=False, encoding='utf_8_sig')
    print(f"\n详细报告已保存至: {filename}")

if __name__ == "__main__":
    # 设定回测区间
    run_backtest(start_date='20250101', end_date=datetime.datetime.now().strftime("%Y%m%d"))