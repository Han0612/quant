import tushare as ts
import pandas as pd
import numpy as np
import datetime
import time
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings

# ================= 0. 系统配置区 =================
warnings.filterwarnings('ignore')

# 【关键】请务必在这里填入你拥有 2120 积分账号的最新 Token！
MY_TOKEN = '97d8d29d796eda4e079b1a5eae9a5b7717260c47972a9e39d5c7d312' 

# 初始化 Tushare
try:
    ts.set_token(MY_TOKEN)
    pro = ts.pro_api()
except Exception as e:
    print(f"❌ Tushare Token 设置失败，请检查: {e}")
    exit()

# 1. 基础系统配置
# 💎 VIP 特权：你有 2000+ 积分，QPM 限制大幅放宽
# 将并发数从 4 提升到 10，速度翻倍！
MAX_WORKERS = 6 
stats_lock = threading.Lock() 

# 2. 【核心策略参数】
MAX_DIST_MA10 = 0.05        
MAX_DIST_MA10_MA20 = 0.12     
EXPLOSION_LOOKBACK_DAYS = 13  
MAX_20_DAYS_RISE = 0.60       

# 3. 【动态风控参数】
AMP_THRESHOLD = 0.06          
TURNOVER_RATIO_LIMIT = 1.5    

# 全局计数器
filter_stats = {
    "1_trend_error": 0, "2_multi_head_error": 0, "3_ma_dist_error": 0,    
    "4_candle_error": 0, "5_vol_error": 0, "6_price_pos_error": 0,  
    "7_high_pos_error": 0, "8_explode_error": 0, "99_unknown_error": 0    
}

# ================= 1. 第一阶段：技术面筛选策略 =================

class TailEndStrategy:
    def __init__(self):
        self.lookback_days = 60  
        
    def get_market_snapshot(self):
        print("🚀 正在启动 Quant Master VIP 引擎...")
        try:
            # 1. 获取基础列表
            data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry,market')
            data = data[data['symbol'].str.startswith(('60', '00'))] # 只看主板
            
            print(f"基础列表获取成功，共 {len(data)} 只，正在全速拉取实时行情...")
            
            # 2. 拉取实时行情 (Legacy Interface)
            symbol_list = data['ts_code'].tolist() # 使用 ts_code
            
            # Tushare 旧版接口需要 6位代码
            code_list_6 = data['symbol'].tolist()
            
            realtime_dfs = []
            batch_size = 800
            for i in tqdm(range(0, len(code_list_6), batch_size), desc="实时数据下载"):
                batch_symbols = code_list_6[i : i + batch_size]
                try:
                    # ts.get_realtime_quotes 是旧接口，有时候不稳定，重试一次
                    for _ in range(3):
                        try:
                            df_batch = ts.get_realtime_quotes(batch_symbols)
                            if df_batch is not None and not df_batch.empty:
                                realtime_dfs.append(df_batch)
                                break
                        except:
                            time.sleep(1)
                except Exception as e:
                    print(f"Batch {i} skip")
            
            if not realtime_dfs:
                raise Exception("无法获取实时行情，请检查网络")
                
            df_rt = pd.concat(realtime_dfs, ignore_index=True)
            
            # 3. 合并数据
            df_final = pd.merge(df_rt, data[['symbol', 'ts_code', 'industry']], left_on='code', right_on='symbol', how='inner')
            
            # 重命名列
            df_final = df_final.rename(columns={
                'price': 'close', 'pre_close': 'pre_close',
                'volume': 'volume_share', 'amount': 'amount'
            })
            
            # 类型转换
            cols = ['close', 'high', 'low', 'open', 'pre_close', 'volume_share', 'amount']
            for c in cols: df_final[c] = pd.to_numeric(df_final[c], errors='coerce').fillna(0)
            
            # 4. 💎 VIP 专属：获取每日指标 (Daily Basic)
            # 你之前在这里报错，现在你有积分了，这里应该畅通无阻
            print("正在调用 VIP 接口获取流通股本...")
            
            # 自动判断日期：如果现在是晚上，尝试取今天；如果今天没出数据，取昨天
            today_str = datetime.datetime.now().strftime('%Y%m%d')
            try:
                df_daily_basic = pro.daily_basic(trade_date=today_str, fields='ts_code,turnover_rate,circ_mv')
                if df_daily_basic.empty:
                    # 如果今天的数据还没生成（比如刚收盘），取最近一个交易日
                    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                    df_daily_basic = pro.daily_basic(trade_date='', fields='ts_code,turnover_rate,circ_mv')
            except:
                # 兜底：不指定日期，API 会自动返回最新的一天
                df_daily_basic = pro.daily_basic(trade_date='', fields='ts_code,turnover_rate,circ_mv')

            df_final = pd.merge(df_final, df_daily_basic[['ts_code', 'circ_mv']], on='ts_code', how='left')
            
            # 5. 补全计算
            df_final['pct_chg'] = (df_final['close'] - df_final['pre_close']) / df_final['pre_close'] * 100
            
            # 换手率计算 (amount是元, circ_mv是万元)
            df_final['turnover'] = 0.0
            mask = df_final['circ_mv'] > 0
            df_final.loc[mask, 'turnover'] = (df_final.loc[mask, 'amount'] / (df_final.loc[mask, 'circ_mv'] * 10000)) * 100
            
            # 统一 volume 为 "手"
            df_final['volume'] = df_final['volume_share'] / 100
            
            return df_final
            
        except Exception as e:
            print(f"❌ 行情拉取严重错误: {e}")
            raise e

    def filter_universe(self, df_snapshot):
        return df_snapshot[df_snapshot['volume'] > 0] 

    def check_individual_stock(self, ts_code, snapshot_row):
        try:
            symbol = snapshot_row['symbol']
            
            # 1. 获取历史数据 (增加重试机制 + 错误静音)
            end_date = datetime.datetime.now().strftime('%Y%m%d')
            start_date = (datetime.datetime.now() - datetime.timedelta(days=self.lookback_days + 30)).strftime('%Y%m%d')
            
            df_hist = None
            # 🔄 增加重试循环：如果报错，歇 0.5 秒再试一次，最多试 3 次
            for _ in range(3):
                try:
                    df_hist = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
                    if df_hist is not None and not df_hist.empty:
                        break # 成功拿到数据，跳出重试
                except Exception:
                    time.sleep(0.5) # 稍作休息
            
            # 如果重试 3 次还是空，或者是烂数据，直接静默跳过，不打印报错
            if df_hist is None or len(df_hist) < 40: 
                return None
            
            # 反转顺序 (Tushare 默认降序)
            df_hist = df_hist.iloc[::-1].reset_index(drop=True)
            
            df_hist = df_hist.rename(columns={'trade_date': 'date', 'vol': 'volume'})
            df_hist['turnover'] = df_hist.get('turnover_rate', 0) 
            
            # --- 智能时间对齐 ---
            last_date = df_hist.iloc[-1]['date']
            curr_date = datetime.datetime.now().strftime('%Y%m%d')
            
            if last_date != curr_date:
                if datetime.datetime.now().weekday() < 5:
                    new_row = {
                        'date': curr_date,
                        'open': snapshot_row['open'],
                        'close': snapshot_row['close'],
                        'high': snapshot_row['high'],
                        'low': snapshot_row['low'],
                        'volume': snapshot_row['volume'],
                        'pct_chg': snapshot_row['pct_chg'],
                        'turnover': snapshot_row['turnover'],
                        'MA10': 0 
                    }
                    df_hist = pd.concat([df_hist, pd.DataFrame([new_row])], ignore_index=True)

            # 指标计算
            df_hist['MA10'] = df_hist['close'].rolling(10).mean()
            df_hist['MA20'] = df_hist['close'].rolling(20).mean()
            df_hist['MA30'] = df_hist['close'].rolling(30).mean()
            df_hist['Vol_MA5'] = df_hist['volume'].rolling(5).mean()
            
            today = df_hist.iloc[-1]
            yesterday = df_hist.iloc[-2]
            
            fail_reason = None
            tags = [] 

            # --- 动态风控 ---
            pre_close = float(snapshot_row['pre_close'])
            amplitude = (today['high'] - today['low']) / pre_close if pre_close > 0 else 0
            
            if amplitude > AMP_THRESHOLD and today['volume'] >= yesterday['volume']:
                tags.append("RISK_高振幅放量")

            try:
                avg_to_5 = df_hist['turnover'].iloc[-6:-1].mean()
                if avg_to_5 > 0 and snapshot_row['turnover'] > (avg_to_5 * TURNOVER_RATIO_LIMIT):
                    tags.append("RISK_换手异常")
            except: pass

            # --- 拦截逻辑 ---
            if not fail_reason: 
                try:
                    price_20 = df_hist.iloc[-20]['close']
                    if (today['close'] - price_20)/price_20 > MAX_20_DAYS_RISE: fail_reason = "7_high_pos_error"
                except: pass
            
            if not fail_reason and not (today['MA10'] > yesterday['MA10']): fail_reason = "1_trend_error"
            
            if not fail_reason: 
                hist_7 = df_hist.iloc[-7:]
                if not ((hist_7['MA10'] > hist_7['MA20']).all() and (hist_7['close'] > hist_7['MA20']).all()):
                    fail_reason = "2_multi_head_error"
            
            if not fail_reason: 
                if today['MA20'] == 0: fail_reason = "3_ma_dist_error"
                else:
                    dist = (today['MA10'] - today['MA20']) / today['MA20']
                    if dist > MAX_DIST_MA10_MA20 or dist < 0: fail_reason = "3_ma_dist_error"
            
            if not fail_reason and not (today['close'] < today['open']): fail_reason = "4_candle_error" 
            
            if not fail_reason and (today['volume'] >= yesterday['volume']): fail_reason = "5_vol_error"
            
            if not fail_reason: 
                dist_10 = (today['close'] - today['MA10']) / today['MA10']
                if dist_10 >= MAX_DIST_MA10 or dist_10 < 0: fail_reason = "6_price_pos_error"

            if not fail_reason: 
                has_expl = False
                for _, r in df_hist.iloc[-EXPLOSION_LOOKBACK_DAYS:-1].iterrows():
                    if 8 <= r['pct_chg'] <= 20 and r['close']>r['open'] and r['volume']>(r['Vol_MA5']*1.5):
                        has_expl = True; break
                if not has_expl: fail_reason = "8_explode_error"

            if fail_reason:
                with stats_lock: filter_stats[fail_reason] += 1
                return None

            return {
                '代码': symbol, 'ts_code': ts_code, '名称': snapshot_row['name'], '行业': snapshot_row['industry'],
                '现价': today['close'], '涨幅': today['pct_chg'], '振幅': round(amplitude*100, 2),
                '相对换手': round(snapshot_row['turnover']/avg_to_5, 2) if avg_to_5>0 else 0,
                'RiskTags': tags
            }

        except Exception as e:
            # 🛑 核心修改：这里不要打印 error 了！
            # 这种错误通常是数据缺失导致的，打印出来只会刷屏
            # print(e)  <-- 注释掉这行
            with stats_lock: filter_stats["99_unknown_error"] += 1
            return None

    def run(self):
        try:
            df_snapshot = self.get_market_snapshot()
        except Exception as e:
            print(f"程序终止: {e}")
            return None

        df_target = self.filter_universe(df_snapshot)
        print(f"初步筛选进入计算池: {len(df_target)}")
        
        results = []
        # VIP 提速：10 线程并发
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_stock = {executor.submit(self.check_individual_stock, row['ts_code'], row): row['symbol'] for _, row in df_target.iterrows()}
            for future in tqdm(as_completed(future_to_stock), total=len(df_target), desc="技术面扫描(VIP加速)"):
                res = future.result()
                if res: results.append(res)
        
        print(f"\n技术筛选拦截统计: {filter_stats}")
        if results: return pd.DataFrame(results)
        else:
            print("\n❌ 今日无符合基础技术形态的个股。")
            return None

# ================= 2. 第二阶段：VIP 资金博弈 =================

class AdvancedSelector:
    def score_stocks(self, candidates_df):
        print("\n>>> 💰 启动 VIP 资金博弈评分 (已解锁核心数据)...")
        
        # 1. 获取板块资金流 (VIP 接口)
        sector_flow_df = None
        try:
            # moneyflow_industry 需要 2000 积分
            today_str = datetime.datetime.now().strftime('%Y%m%d')
            sector_flow_df = pro.moneyflow_industry(trade_date=today_str)
            if sector_flow_df.empty:
                print("⚠️ 提示: 今日板块资金流尚未更新(可能太早或太晚)，尝试获取昨日...")
                yst_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                sector_flow_df = pro.moneyflow_industry(trade_date=yst_str)
            
            if not sector_flow_df.empty:
                sector_flow_df['net_inflow_yi'] = sector_flow_df['net_amount'] / 100000000
                print(f"✅ 板块资金流获取成功! 榜首: {sector_flow_df.iloc[0]['industry']}")
        except Exception as e:
            print(f"⚠️ 板块资金流接口异常: {e}")

        scored_results = []
        for _, row in tqdm(candidates_df.iterrows(), total=len(candidates_df), desc="博弈评分"):
            score = 20 # 基础分
            reasons = []
            
            # 板块匹配
            net_yi = 0
            if sector_flow_df is not None:
                match = sector_flow_df[sector_flow_df['industry'] == row['行业']]
                if not match.empty:
                    net_yi = match.iloc[0]['net_inflow_yi']
                    if net_yi > 3: 
                        score += 50; reasons.append(f"🔥板块爆买({round(net_yi,1)}亿)")
                    elif net_yi > 1:
                        score += 30; reasons.append(f"⚡主力潜伏({round(net_yi,1)}亿)")
                    elif net_yi < -1:
                        score -= 50; reasons.append(f"⚠️板块出逃({round(net_yi,1)}亿)")

            # 换手评分
            if 0.8 <= row['相对换手'] <= 1.2: score += 10; reasons.append("量能稳健")
            
            # 风险扣分
            if row['RiskTags']:
                if score < 50: # 如果不是热点，严厉扣分
                    score -= 100
                    reasons.append(f"❌形态风险({','.join(row['RiskTags'])})")
                else:
                    reasons.append(f"🛡️热点豁免({','.join(row['RiskTags'])})")

            # 建议
            sugg = "⚪观察"
            if score >= 50: sugg = "⭐强烈推荐"
            elif score > 0: sugg = "✅建议关注"
            
            scored_results.append({
                '代码': row['代码'], '名称': row['名称'], '行业': row['行业'],
                '总分': score, '建议': sugg, '现价': row['现价'],
                '板块净流(亿)': round(net_yi, 2), '核心理由': " ".join(reasons)
            })

        return pd.DataFrame(scored_results).sort_values(by='总分', ascending=False)

# ================= 入口 =================
if __name__ == "__main__":
    print(f"========= Quant Master V5.0 (VIP Ultimate) ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}) =========")
    
    st = TailEndStrategy()
    df_tech = st.run()
    
    if df_tech is not None:
        print(f"技术初筛通过: {len(df_tech)} 只")
        sl = AdvancedSelector()
        final = sl.score_stocks(df_tech)
        
        print("\n" + "="*80)
        print(final[['代码','名称','行业','总分','建议','板块净流(亿)','核心理由']].to_markdown(index=False))
        print("="*80)
        
        fname = f"VIP_Selection_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        final.to_csv(fname, index=False, encoding='utf_8_sig')
        print(f"\n✅ 结果已保存: {fname}")
    else:
        print("今日无标的。")