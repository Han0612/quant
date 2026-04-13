import tushare as ts
import pandas as pd
import numpy as np
import datetime
import time
import os
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings

# ================= 0. 系统配置区 =================
warnings.filterwarnings('ignore')

# 🛑【关键限速】
# 代理通道带宽有限，绝对不能开 20 线程！
MAX_WORKERS = 5  
stats_lock = threading.Lock() 

# ================= 🔑 Tushare 初始化 =================
print(f"🚀 启动 Quant Master V7.0 (Proxy Stable) 引擎...")

try:
    pro = ts.pro_api('init_placeholder')
    # 💉 注入专用通道 (你必须走代理)
    pro._DataApi__token     = '5026271042408398727'
    pro._DataApi__http_url  = 'http://5k1a.xiximiao.com/dataapi'
    print("✅ 专用数据通道已激活。")
    print(f"🐌 为了保证不丢包，并发数已限制为: {MAX_WORKERS} (速度稍慢，请耐心等待)")
        
except Exception as e:
    print(f"❌ Tushare 初始化失败: {e}")
    exit()

# ================= 2. 核心策略参数 =================
MAX_DIST_MA10 = 0.02       
MAX_DIST_MA10_MA20 = 0.12     
EXPLOSION_LOOKBACK_DAYS = 12  
MAX_20_DAYS_RISE = 0.60       
AMP_THRESHOLD = 0.06          
TURNOVER_RATIO_LIMIT = 1.5    

filter_stats = {
    "0_data_missing": 0,      
    "1_trend_error": 0,       
    "2_multi_head_error": 0,  
    "3_ma_dist_error": 0,     
    "4_candle_error": 0,      
    "5_vol_error": 0,         
    "6_price_pos_error": 0,   
    "7_high_pos_error": 0,    
    "8_explode_error": 0,     
    "99_unknown_error": 0     
}

# ================= 1. 第一阶段：技术面筛选 =================

class TailEndStrategy:
    def __init__(self):
        self.lookback_days = 60  
        
    def get_market_snapshot(self):
        try:
            # 1. 获取基础列表
            print("正在请求基础股票列表...")
            # 增加重试
            data = None
            for _ in range(3):
                try:
                    data = pro.stock_basic(exchange='', list_status='L')
                    if not data.empty and 'symbol' in data.columns: break
                except: time.sleep(1)
            
            if data is None or data.empty: raise Exception("基础列表获取失败")

            data = data[data['symbol'].str.startswith(('60', '00'))] 
            print(f"基础列表获取成功 ({len(data)} 只)。正在拉取实时行情...")
            
            # 2. 拉取实时行情
            code_list_6 = data['symbol'].tolist()
            realtime_dfs = []
            batch_size = 500 # 降低单次包大小
            
            for i in tqdm(range(0, len(code_list_6), batch_size), desc="实时数据"):
                batch_symbols = code_list_6[i : i + batch_size]
                try:
                    # 随机延迟，防止拥堵
                    time.sleep(random.uniform(0.05, 0.1))
                    df_batch = ts.get_realtime_quotes(batch_symbols)
                    if df_batch is not None and not df_batch.empty:
                        if 'code' in df_batch.columns:
                            realtime_dfs.append(df_batch)
                except: pass 
            
            if not realtime_dfs: raise Exception("无法获取实时行情")
            df_rt = pd.concat(realtime_dfs, ignore_index=True)
            
            if 'code' not in df_rt.columns:
                df_rt.columns = [c.strip() for c in df_rt.columns]
            
            # 3. 合并
            data['symbol'] = data['symbol'].astype(str).str.strip()
            df_rt['code'] = df_rt['code'].astype(str).str.strip()
            
            df_final = pd.merge(df_rt, data[['symbol', 'ts_code', 'industry']], left_on='code', right_on='symbol', how='inner')
            df_final = df_final.rename(columns={'price': 'close', 'pre_close': 'pre_close', 'volume': 'volume_share', 'amount': 'amount'})
            
            cols = ['close', 'high', 'low', 'open', 'pre_close', 'volume_share', 'amount']
            for c in cols: 
                if c in df_final.columns: df_final[c] = pd.to_numeric(df_final[c], errors='coerce').fillna(0)
            
            # 4. 初始化占位列 (游击战模式)
            df_final['circ_mv'] = 0.0
            df_final['turnover'] = 0.0
            
            # 5. 计算基础指标
            df_final['pct_chg'] = (df_final['close'] - df_final['pre_close']) / df_final['pre_close'] * 100
            df_final['volume'] = df_final['volume_share'] / 100
            
            return df_final
            
        except Exception as e:
            print(f"❌ 行情初始化报错: {e}")
            raise e

    def filter_universe(self, df_snapshot):
        return df_snapshot[df_snapshot['volume'] > 0] 

    def check_individual_stock(self, ts_code, snapshot_row):
        try:
            # 🐌 错峰请求：每个线程进来先随机睡一下，避免撞车
            time.sleep(random.uniform(0.01, 0.2))

            # 1. 获取历史 K 线
            df_hist = None
            # 顽强重试 5 次
            for i in range(5): 
                try:
                    end_date = datetime.datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.datetime.now() - datetime.timedelta(days=self.lookback_days + 20)).strftime('%Y%m%d')
                    # 优先用 pro.daily，因为它数据包小
                    df_hist = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                    
                    if df_hist is not None and not df_hist.empty: 
                        # 获取复权因子 (单独请求，成功率更高)
                        adj = None
                        for _ in range(3):
                            try:
                                adj = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
                                if not adj.empty: break
                            except: time.sleep(0.2)
                        
                        if adj is not None and not adj.empty:
                            df_hist = pd.merge(df_hist, adj, on='trade_date', how='left')
                            # 填充缺失因子
                            df_hist['adj_factor'] = df_hist['adj_factor'].fillna(method='ffill')
                            latest_adj = df_hist['adj_factor'].iloc[0]
                            if latest_adj > 0:
                                factor = df_hist['adj_factor'] / latest_adj
                                df_hist['close'] = df_hist['close'] * factor
                                df_hist['open'] = df_hist['open'] * factor
                                df_hist['high'] = df_hist['high'] * factor
                                df_hist['low'] = df_hist['low'] * factor
                        break
                except: 
                    time.sleep(0.5 * (i+1)) # 失败后等待时间递增

            if df_hist is None or len(df_hist) < 40: 
                with stats_lock: filter_stats["0_data_missing"] += 1
                return None
            
            # 2. 单独获取该股票的 daily_basic (股本数据)
            circ_mv = 0
            real_turnover = 0
            try:
                start_db = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y%m%d')
                end_db = datetime.datetime.now().strftime('%Y%m%d')
                # 顽强重试 3 次
                for _ in range(3):
                    try:
                        df_basic = pro.daily_basic(ts_code=ts_code, start_date=start_db, end_date=end_db, fields='trade_date,circ_mv,turnover_rate')
                        if not df_basic.empty:
                            latest_basic = df_basic.iloc[0]
                            circ_mv = latest_basic['circ_mv']
                            if circ_mv > 0:
                                real_turnover = (snapshot_row['amount'] / (circ_mv * 10000)) * 100
                            break
                    except: time.sleep(0.2)
            except: pass
            
            df_hist = df_hist.sort_values('trade_date', ascending=True).reset_index(drop=True)
            df_hist = df_hist.rename(columns={'trade_date': 'date', 'vol': 'volume'})
            
            # 时间对齐
            if df_hist.iloc[-1]['date'] != datetime.datetime.now().strftime('%Y%m%d'):
                if datetime.datetime.now().weekday() < 5:
                    new_row = {
                        'date': datetime.datetime.now().strftime('%Y%m%d'),
                        'close': snapshot_row['close'], 'open': snapshot_row['open'],
                        'high': snapshot_row['high'], 'low': snapshot_row['low'],
                        'volume': snapshot_row['volume'], 'pct_chg': snapshot_row['pct_chg'],
                        'MA10': 0
                    }
                    df_hist = pd.concat([df_hist, pd.DataFrame([new_row])], ignore_index=True)

            df_hist['MA10'] = df_hist['close'].rolling(10).mean()
            df_hist['MA20'] = df_hist['close'].rolling(20).mean()
            df_hist['MA30'] = df_hist['close'].rolling(30).mean()
            df_hist['Vol_MA5'] = df_hist['volume'].rolling(5).mean()
            
            today = df_hist.iloc[-1]
            yesterday = df_hist.iloc[-2]
            fail_reason = None
            tags = [] 

            pre_close = float(snapshot_row['pre_close'])
            amp = (today['high'] - today['low']) / pre_close if pre_close > 0 else 0
            if amp > AMP_THRESHOLD and today['volume'] >= yesterday['volume']: tags.append("RISK_高振幅放量")
            
            vol_ratio = today['volume'] / (df_hist['volume'].iloc[-6:-1].mean() + 1)
            if vol_ratio > TURNOVER_RATIO_LIMIT:
                tags.append("RISK_换手异常")

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
                dist = (today['close'] - today['MA10']) / today['MA10']
                if dist >= MAX_DIST_MA10 or dist < 0: fail_reason = "6_price_pos_error"
            
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
                '代码': ts_code, '名称': snapshot_row['name'], '行业': snapshot_row['industry'],
                'circ_mv': circ_mv, 
                '现价': today['close'], '涨幅': today['pct_chg'], 
                'RiskTags': tags, '相对换手': round(vol_ratio, 2),
                'Turnover': real_turnover
            }
        except:
            with stats_lock: filter_stats["99_unknown_error"] += 1
            return None

    def run(self):
        try:
            df_snapshot = self.get_market_snapshot()
            df_target = self.filter_universe(df_snapshot)
            print(f"初步筛选进入计算池: {len(df_target)}")
            
            results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_stock = {executor.submit(self.check_individual_stock, row['ts_code'], row): row['symbol'] for _, row in df_target.iterrows()}
                for future in tqdm(as_completed(future_to_stock), total=len(df_target), desc="技术面稳健扫描"):
                    res = future.result()
                    if res: results.append(res)
            
            print("\n" + "-"*30)
            print("📊 淘汰原因统计")
            print("-"*30)
            sorted_stats = dict(sorted(filter_stats.items(), key=lambda item: item[1], reverse=True))
            total_stats = 0
            for reason, count in sorted_stats.items():
                print(f"❌ {reason:<20}: {count}")
                total_stats += count
            print("-"*30)
            print(f"统计总数: {total_stats} / 样本总数: {len(df_target)}")
            print(f"差异 (通过初筛): {len(results)}")
            
            if results: return pd.DataFrame(results)
            else: return None
        except Exception as e:
            return None

# ================= 2. 第二阶段：God Tier 资金博弈 =================

class AdvancedSelector:
    def score_stocks(self, candidates_df):
        print("\n>>> 💰 启动 God Tier 资金深度审计...")
        now_hour = datetime.datetime.now().hour
        is_intraday = (9 <= now_hour < 15)
        
        sector_flow_df = None
        try:
            # 兼容代理模式
            # 如果是代理模式，moneyflow_industry 可能也会卡，增加重试
            today_str = datetime.datetime.now().strftime('%Y%m%d')
            query_date = today_str
            if is_intraday:
                print("⚠️ 盘中模式：板块资金流使用【昨日数据】作为参考。")
                query_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                
            for _ in range(3): # 重试
                try:
                    sector_flow_df = pro.moneyflow_industry(trade_date=query_date)
                    if not sector_flow_df.empty: break
                except: time.sleep(1)
            
            if sector_flow_df.empty: 
                 for delta in range(1, 4):
                     d_str = (datetime.datetime.now() - datetime.timedelta(days=delta)).strftime('%Y%m%d')
                     try: sector_flow_df = pro.moneyflow_industry(trade_date=d_str)
                     except: pass
                     if not sector_flow_df.empty: break

            if not sector_flow_df.empty:
                sector_flow_df['net_inflow_yi'] = sector_flow_df['net_amount'] / 100000000
                print(f"✅ 板块资金流就位。")
        except: pass

        scored_results = []
        for _, row in tqdm(candidates_df.iterrows(), total=len(candidates_df), desc="主力行为审计"):
            score = 0 
            reasons = []
            ts_code = row['代码']
            risk_tags = row['RiskTags']
            
            net_yi = 0
            sector_is_hot = False 
            if sector_flow_df is not None:
                match = sector_flow_df[sector_flow_df['industry'] == row['行业']]
                if not match.empty:
                    net_yi = match.iloc[0]['net_inflow_yi']
                    if net_yi > 3.0: 
                        score += 50; reasons.append(f"🔥资金爆买({round(net_yi,1)}亿)")
                        sector_is_hot = True
                    elif net_yi > 1.0:
                        score += 30; reasons.append(f"⚡主力潜伏({round(net_yi,1)}亿)")
                        sector_is_hot = True
                    elif net_yi < -1.0: 
                        score -= 50; reasons.append(f"⚠️资金出逃({round(net_yi,1)}亿)")

            risk_penalty = False
            if risk_tags:
                if sector_is_hot:
                    reasons.append(f"🛡️热点豁免({','.join(risk_tags)})")
                else:
                    score -= 100 
                    risk_penalty = True
                    reasons.append(f"❌形态恶劣({','.join(risk_tags)})")

            mv_yi = row['circ_mv'] / 10000 
            if 30 <= mv_yi <= 120:
                score += 20; reasons.append(f"💰黄金市值({round(mv_yi,1)}亿)")
            elif mv_yi < 30:
                score += 10; reasons.append(f"微盘股({round(mv_yi,1)}亿)")
            elif mv_yi > 120:
                score += 5; reasons.append(f"大盘股({round(mv_yi,1)}亿)")

            to = row['Turnover']
            if 5 <= to <= 15:
                score += 20; reasons.append(f"⚡活跃换手({round(to,1)}%)")
            elif 3 <= to < 5:
                score += 10; reasons.append(f"温和换手")
            elif to > 20:
                score -= 5; reasons.append(f"换手过热")

            if not is_intraday and not risk_penalty: 
                try:
                    # 代理通道下 moneyflow 也可能卡，增加重试
                    for _ in range(2):
                        try:
                            end_d = datetime.datetime.now().strftime('%Y%m%d')
                            start_d = (datetime.datetime.now() - datetime.timedelta(days=10)).strftime('%Y%m%d')
                            df_flow = pro.moneyflow(ts_code=ts_code, start_date=start_d, end_date=end_d)
                            if not df_flow.empty:
                                total_net_inflow = df_flow.head(5)['net_mf_amount'].sum()
                                if total_net_inflow > 1000: score += 30; reasons.append(f"🐳主力近5日吸筹")
                                elif total_net_inflow < -2000: score -= 30; reasons.append(f"💸主力出货")
                                break
                        except: time.sleep(0.5)
                except: pass

            sugg = "⚪观察"
            if score >= 50: sugg = "⭐强烈推荐"
            elif score > 0: sugg = "✅建议关注"
            if risk_penalty: sugg = "⚠️需确认风口"
            
            scored_results.append({
                '代码': row['代码'], '名称': row['名称'], '行业': row['行业'],
                '总分': score, '建议': sugg, '现价': row['现价'],
                '核心理由': " ".join(reasons)
            })

        return pd.DataFrame(scored_results).sort_values(by='总分', ascending=False)

if __name__ == "__main__":
    print(f"========= Quant Master V7.0 (Proxy Stable) ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}) =========")
    st = TailEndStrategy()
    df_tech = st.run()
    
    if df_tech is not None:
        print(f"技术初筛通过: {len(df_tech)} 只")
        sl = AdvancedSelector()
        final = sl.score_stocks(df_tech)
        
        print("\n" + "="*80)
        print(final[['代码','名称','行业','总分','建议','核心理由']].to_markdown(index=False))
        print("="*80)
        fname = f"GodTier_Selection_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        final.to_csv(fname, index=False, encoding='utf_8_sig')
        print(f"\n✅ 结果已保存: {fname}")
    else:
        print("今日无符合条件的个股。")