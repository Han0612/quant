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
    "99_unknown_error": 0
}

# ================= 🔑 Tushare 初始化 =================
print(f"🚀 启动 Quant Master V7.2 (Optimized) 引擎...")

try:
    pro = ts.pro_api('init_placeholder')
    pro._DataApi__token     = '4502048184048033923'
    pro._DataApi__http_url  = 'http://5k1a.xiximiao.com/dataapi'
    print("✅ 专用数据通道已激活。")
    print(f"🐌 并发限制: {MAX_WORKERS} (稳定优先)")
        
except Exception as e:
    print(f"❌ Tushare 初始化失败: {e}")
    exit()

# ╔══════════════════════════════════════════════════════════════╗
# ║              策略参数 (优化器最优解)                          ║
# ║  来源: 5年数据坐标下降 + Walk-Forward验证                     ║
# ║  退出: TP1=+6% TP2=+8% SL=-5% Hold=2天                     ║
# ╚══════════════════════════════════════════════════════════════╝

# --- 爆量参数 ---
EXPLOSION_LOOKBACK_DAYS = 16       # [优化] 14→16 回溯窗口
EXPLOSION_PCT_MIN  = 7.5           # [优化] 8.0→7.5 最低涨幅
EXPLOSION_PCT_MAX  = 16.0          # [优化] 20→16 排除过度投机
EXPLOSION_VOL_MULT = 1.4           # [优化] 1.2→1.4 更高质量放量

# --- 均线排列 ---
BULL_DAYS_WINDOW     = 8           # [优化] 4→8 检查窗口
BULL_DAYS_THRESHOLD  = 8           # [优化] 3→8 要求8/8天多头
MA20_MA30_TOLERANCE  = 1.008       # [优化] 0.995→1.008 要求MA20明确高于MA30
MA10_SLOPE_TOLERANCE = 0.993       # [优化] 0.999→0.993 允许MA10微降0.7%

# --- 距离/位置 ---
MA30_SUPPORT_TOLERANCE = 0.98      # [优化] 0.99→0.98 MA30支撑容差放宽
MAX_DIST_MA10      = 0.025         # [优化] 0.02→0.025 击球区上沿
MAX_DIST_MA10_LOW  = 0.015          # [优化] -0.01→0.015 击球区下沿
MAX_DIST_MA10_MA20 = 0.025         # [优化] 0.04→0.025 乖离率收紧
MAX_20_DAYS_RISE   = 0.35          # [优化] 0.60→0.35 防追高收紧

# --- 风控标记 (不变) ---
AMP_THRESHOLD = 0.06
TURNOVER_RATIO_LIMIT = 1.5

# ================= 3. 辅助函数 =================

def get_intraday_volume_completion(minutes_elapsed):
    """A股日内成交量U型分布模型"""
    CURVE = [
        (0,   0.000),
        (10,  0.055),
        (20,  0.100),
        (30,  0.145),
        (45,  0.195),
        (60,  0.245),
        (90,  0.325),
        (120, 0.420),
        (135, 0.480),
        (150, 0.535),
        (165, 0.590),
        (180, 0.645),
        (195, 0.715),
        (210, 0.790),
        (220, 0.850),
        (230, 0.920),
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
            ratio = (minutes_elapsed - t0) / (t1 - t0)
            return r0 + ratio * (r1 - r0)
    return 1.0


def calc_trading_minutes_elapsed():
    """计算当前时刻距开盘已过交易分钟数 (0~240)"""
    now = datetime.datetime.now()
    if now.weekday() >= 5:
        return 240
    h, m = now.hour, now.minute
    current_minutes = h * 60 + m
    open_am = 9 * 60 + 30
    close_am = 11 * 60 + 30
    open_pm = 13 * 60
    close_pm = 15 * 60
    if current_minutes < open_am:
        return 0
    elif current_minutes <= close_am:
        return current_minutes - open_am
    elif current_minutes < open_pm:
        return 120
    elif current_minutes <= close_pm:
        return 120 + (current_minutes - open_pm)
    else:
        return 240


def project_full_day_volume(current_volume, minutes_elapsed):
    """基于U型曲线的全天成交量预测"""
    completion = get_intraday_volume_completion(minutes_elapsed)
    return current_volume / max(completion, 0.01)


def get_daily_basic_bulk():
    """批量拉取全市场基本面指标"""
    print("⚡ 正在批量拉取全市场基本面指标...")
    for delta in range(10): 
        d_str = (datetime.datetime.now() - datetime.timedelta(days=delta)).strftime('%Y%m%d')
        try:
            df_basics = pro.daily_basic(trade_date=d_str, fields='ts_code,float_share,circ_mv,pe_ttm,turnover_rate')
            if not df_basics.empty and len(df_basics) > 2000:
                print(f"✅ 成功命中 {d_str} 基础数据，共 {len(df_basics)} 条")
                df_basics = df_basics.rename(columns={'turnover_rate': 'last_turnover'})
                return df_basics
        except: 
            time.sleep(0.5)
    print("⚠️ 警告：无法获取 daily_basic。")
    return pd.DataFrame()

# ================= 4. 第一阶段：技术面筛选 =================

class TailEndStrategy:
    def __init__(self):
        self.lookback_days = 80  # [优化] 60→80 确保bull_8d和MA30有足够预热

    def get_market_snapshot(self):
        try:
            print("正在请求基础股票列表...")
            data = None
            for attempt in range(3):
                try:
                    data = pro.stock_basic(exchange='', list_status='L')
                    if data is not None and not data.empty and len(data) > 4000:
                        break
                    else:
                        print(f"⚠️ 数据量异常 ({len(data) if data is not None else 0}条), 重试...")
                        time.sleep(1)
                except Exception as e:
                    print(f"⚠️ 请求失败 (第{attempt+1}次): {e}")
                    time.sleep(1)
            
            if data is None or data.empty or len(data) < 4000: 
                raise Exception("基础列表获取失败")

            data = data[data['symbol'].str.startswith(('60', '00'))] 
            print(f"基础列表获取成功 ({len(data)} 只)。")

            df_basics = get_daily_basic_bulk()
            
            print("正在拉取实时行情...")
            code_list_6 = data['symbol'].tolist()
            realtime_dfs = []
            batch_size = 100 
            
            for i in tqdm(range(0, len(code_list_6), batch_size), desc="实时数据下载"):
                batch_symbols = code_list_6[i : i + batch_size]
                success = False
                for attempt in range(3):
                    try:
                        time.sleep(random.uniform(0.05, 0.2)) 
                        df_batch = ts.get_realtime_quotes(batch_symbols)
                        if df_batch is not None and not df_batch.empty:
                            if 'code' in df_batch.columns:
                                realtime_dfs.append(df_batch)
                                success = True
                                break
                    except Exception as e:
                        print(f"⚠️ 批次异常 (Retry {attempt+1}): {e}")
                        time.sleep(1)
                if not success:
                    print(f"❌ 丢包: {len(batch_symbols)} 只股票数据丢失！")

            if not realtime_dfs: 
                raise Exception("无法获取任何实时行情")
            
            df_rt = pd.concat(realtime_dfs, ignore_index=True)
            
            if len(df_rt) < len(code_list_6) * 0.9:
                print(f"⚠️ 数据完整性警告: 计划 {len(code_list_6)} 只，实际 {len(df_rt)} 只。")
            
            if 'code' not in df_rt.columns:
                df_rt.columns = [c.strip() for c in df_rt.columns]
            
            data['symbol'] = data['symbol'].astype(str).str.strip()
            df_rt['code'] = df_rt['code'].astype(str).str.strip()
            
            df_final = pd.merge(df_rt, data[['symbol', 'ts_code', 'industry']], left_on='code', right_on='symbol', how='inner')
            
            if not df_basics.empty:
                df_final = pd.merge(df_final, df_basics, on='ts_code', how='left')
                df_final['float_share'] = df_final['float_share'].fillna(0)
                df_final['circ_mv'] = df_final['circ_mv'].fillna(0)
            else:
                df_final['float_share'] = 0
                df_final['circ_mv'] = 0

            df_final = df_final.rename(columns={'price': 'close', 'volume': 'volume_raw', 'amount': 'amount_raw'})

            cols = ['close', 'high', 'low', 'open', 'pre_close', 'volume_raw', 'amount_raw']
            for c in cols: 
                if c in df_final.columns: df_final[c] = pd.to_numeric(df_final[c], errors='coerce').fillna(0)
            
            df_final['pre_close'] = df_final['pre_close'].replace(0, np.nan) 
            df_final['pct_chg'] = (df_final['close'] - df_final['pre_close']) / df_final['pre_close'] * 100
            df_final['pct_chg'] = df_final['pct_chg'].fillna(0)
            df_final['volume'] = df_final['volume_raw'] / 100
            df_final['amount'] = df_final['amount_raw'] / 1000

            return df_final
            
        except Exception as e:
            print(f"❌ 行情初始化报错: {e}")
            raise e

    def filter_universe(self, df_snapshot):
        return df_snapshot[df_snapshot['volume'] > 0] 

    def check_individual_stock(self, ts_code, snapshot_row):
        try:
            time.sleep(random.uniform(0.01, 0.2))

            # ================= 1. 获取历史 K 线 =================
            df_hist = None
            for i in range(4): 
                try:
                    end_date = datetime.datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.datetime.now() - datetime.timedelta(days=self.lookback_days + 20)).strftime('%Y%m%d')
                    
                    df_hist = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                    
                    if df_hist is not None and not df_hist.empty: 
                        adj = None
                        for _ in range(3):
                            try:
                                adj = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
                                if not adj.empty: break
                            except: time.sleep(0.1)
                        
                        if adj is not None and not adj.empty:
                            df_hist = pd.merge(df_hist, adj, on='trade_date', how='left')
                            df_hist = df_hist.sort_values('trade_date', ascending=False).reset_index(drop=True)
                            df_hist['adj_factor'] = df_hist['adj_factor'].fillna(method='ffill')
                            latest_adj = df_hist['adj_factor'].iloc[0]
                            
                            if latest_adj > 0:
                                factor = df_hist['adj_factor'] / latest_adj
                                cols_to_adj = ['close', 'open', 'high', 'low', 'pre_close']
                                for col in cols_to_adj:
                                    if col in df_hist.columns:
                                        df_hist[col] = df_hist[col] * factor
                        else:
                            df_hist = df_hist.sort_values('trade_date', ascending=False).reset_index(drop=True)
                            
                        break
                except: 
                    time.sleep(0.5 * (i+1))

            if df_hist is None or len(df_hist) < 40: 
                with stats_lock: filter_stats["0_data_missing"] += 1
                return None
            
            # ================= 2. 实时数据准备 =================
            float_share = snapshot_row.get('float_share', 0)
            circ_mv = snapshot_row.get('circ_mv', 0)
            
            real_turnover = 0
            if float_share > 0:
                real_turnover = snapshot_row['volume'] / float_share
            
            # ================= 3. 历史数据拼接 =================
            df_hist = df_hist.sort_values('trade_date', ascending=True).reset_index(drop=True)
            df_hist = df_hist.rename(columns={'trade_date': 'date', 'vol': 'volume'})
            
            last_close_hist = df_hist.iloc[-1]['close']
            if snapshot_row['close'] < last_close_hist * 0.7:
                 return None

            now_time = datetime.datetime.now()
            current_date_str = now_time.strftime('%Y%m%d')
            
            is_trading_time = (now_time.hour > 9) or (now_time.hour == 9 and now_time.minute > 15)

            snapshot_date = str(snapshot_row['date']).replace('-', '')
            is_data_fresh = (snapshot_date == current_date_str)

            should_append = (
                (df_hist.iloc[-1]['date'] != current_date_str) and
                (now_time.weekday() < 5) and
                is_trading_time and
                is_data_fresh
            )
            
            if should_append:
                new_row = {
                    'date': current_date_str,
                    'close': snapshot_row['close'], 
                    'open': snapshot_row['open'],
                    'high': snapshot_row['high'], 
                    'low': snapshot_row['low'],
                    'volume': snapshot_row['volume'], 
                    'pct_chg': snapshot_row['pct_chg'],
                    'MA10': 0 
                }
                df_hist = pd.concat([df_hist, pd.DataFrame([new_row])], ignore_index=True)

            # 计算均线
            df_hist['MA10'] = df_hist['close'].rolling(10).mean()
            df_hist['MA20'] = df_hist['close'].rolling(20).mean()
            df_hist['MA30'] = df_hist['close'].rolling(30).mean()
            df_hist['Vol_MA5'] = df_hist['volume'].rolling(5).mean()
            
            today = df_hist.iloc[-1]
            yesterday = df_hist.iloc[-2]
            
            # ================= 4. 漏斗筛选 =================
            fail_reason = None
            tags = [] 

            pre_close = float(snapshot_row['pre_close'])
            amp = (today['high'] - today['low']) / pre_close if pre_close > 0 else 0

            minutes_elapsed = calc_trading_minutes_elapsed()
            vol_projected = project_full_day_volume(today['volume'], minutes_elapsed)

            # 风险标记
            if amp > AMP_THRESHOLD and vol_projected >= yesterday['volume']: 
                tags.append("RISK_高振幅放量")
            
            vol_avg_5 = df_hist['volume'].iloc[-6:-1].mean() + 1
            vol_ratio = vol_projected / vol_avg_5
            
            if vol_ratio > TURNOVER_RATIO_LIMIT: 
                tags.append(f"RISK_放量({round(vol_ratio,1)}倍)")

            # ─── 规则1: 爆量基因 ───
            # [优化] 16日回溯, 涨幅7.5%-16%, 量比>1.4
            if not fail_reason:
                has_expl = False
                for _, r in df_hist.iloc[-EXPLOSION_LOOKBACK_DAYS-1:-1].iterrows():
                    pct_ok = (EXPLOSION_PCT_MIN <= r['pct_chg'] <= EXPLOSION_PCT_MAX)
                    vol_ok = (r['volume'] > (r['Vol_MA5'] * EXPLOSION_VOL_MULT)) if r['Vol_MA5'] > 0 else False
                    is_limit_t = (r['close'] == r['open']) and (r['pct_chg'] > 9.5)
                    if (pct_ok and vol_ok) or is_limit_t:
                        has_expl = True
                        break
                if not has_expl: fail_reason = "8_explode_error"

            # ─── 规则2: 多头排列 ───
            # [优化] 8/8天 MA10>MA20 + MA20>MA30×1.008
            if not fail_reason:
                hist_n = df_hist.iloc[-BULL_DAYS_WINDOW:]
                bull_days = (hist_n['MA10'] > hist_n['MA20']).sum()
                
                is_stable_bull = (bull_days >= BULL_DAYS_THRESHOLD)
                
                is_golden_cross = (today['MA10'] > today['MA20']) and \
                                  (yesterday['MA10'] <= yesterday['MA20']) and \
                                  (today['MA10'] > yesterday['MA10'])

                # [优化] MA20必须明确高于MA30 (×1.008)
                is_ma20_above_ma30 = (today['MA20'] > today['MA30'] * MA20_MA30_TOLERANCE)
                
                if not ((is_stable_bull or is_golden_cross) and is_ma20_above_ma30):
                    fail_reason = "2_multi_head_error"

            # ─── 规则3: MA10斜率 ───
            # [优化] 允许微降0.7% (0.993)
            if not fail_reason:
                ma10_is_rising = today['MA10'] >= (yesterday['MA10'] * MA10_SLOPE_TOLERANCE)
                if not ma10_is_rising: 
                    fail_reason = "1_trend_error"

            # ─── 规则4: MA30生命线 ───
            # [优化] 容差2% (0.98)
            if not fail_reason:
                if today['close'] < (today['MA30'] * MA30_SUPPORT_TOLERANCE):
                    fail_reason = "2_multi_head_error" 

            # ─── 规则5: 乖离率 ───
            # [优化] 2.5% (0.025)
            if not fail_reason:
                if today['MA20'] == 0: 
                    fail_reason = "3_ma_dist_error"
                else:
                    dist_ma = (today['MA10'] - today['MA20']) / today['MA20']
                    if dist_ma > MAX_DIST_MA10_MA20: 
                        fail_reason = "3_ma_dist_error"

            # ─── 规则6: 阴线+缩量联合 ───
            if not fail_reason:
                is_pullback = (today['close'] < yesterday['close']) or (today['close'] < today['open'])
                is_shrink = (vol_projected < yesterday['volume'] * 1.0)
                
                if not is_pullback:
                    fail_reason = "4_candle_vol_error"
                elif not is_shrink:
                    fail_reason = "4_candle_vol_error"

            # ─── 规则7: 防追高 ───
            # [优化] 20日涨幅 < 35%
            if not fail_reason:
                try:
                    price_20 = df_hist.iloc[-20]['close']
                    if (today['close'] - price_20)/price_20 > MAX_20_DAYS_RISE: 
                        fail_reason = "7_high_pos_error"
                except: pass

            # ─── 规则8: 击球区 ───
            # [优化] MA10上方 +2.0% ~ +2.5%
            if not fail_reason:
                dist = (today['close'] - today['MA10']) / today['MA10']
                if not (MAX_DIST_MA10_LOW < dist < MAX_DIST_MA10): 
                    fail_reason = "6_price_pos_error"

            # ─── 结算 ───
            if fail_reason:
                with stats_lock: filter_stats[fail_reason] += 1
                return None

            return {
                '代码': ts_code, '名称': snapshot_row['name'], '行业': snapshot_row['industry'],
                'circ_mv': circ_mv, 
                '现价': today['close'], '涨幅': round(today['pct_chg'], 2), 
                'RiskTags': tags, '相对换手': round(vol_ratio, 2),
                'Turnover': round(real_turnover, 2)
            }
        except:
            with stats_lock: filter_stats["99_unknown_error"] += 1
            return None

    def run(self):
        print("🔍 正在获取市场快照...")
        
        df_snapshot = self.get_market_snapshot()
        if df_snapshot is None or df_snapshot.empty:
            print("❌ 无法获取市场快照。策略终止。")
            return None

        df_target = self.filter_universe(df_snapshot)
        if df_target is None or df_target.empty:
            print("⚠️ 初步筛选后无股票入围。")
            return None
            
        print(f"初步筛选进入计算池: {len(df_target)} (已剔除停牌/无量)")
        
        results = []
        
        try:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_stock = {
                    executor.submit(self.check_individual_stock, row['ts_code'], row): row['symbol'] 
                    for _, row in df_target.iterrows()
                }
                
                for future in tqdm(as_completed(future_to_stock), total=len(df_target), desc="Quant Master 深度扫描"):
                    try:
                        res = future.result() 
                        if res: 
                            results.append(res)
                    except KeyboardInterrupt:
                        print("\n🛑 用户中断！")
                        executor.shutdown(wait=False)
                        break
                    except:
                        pass
        except KeyboardInterrupt:
            print("\n🛑 任务终止。")

        print("\n" + "-"*30)
        print("📊 淘汰漏斗统计")
        print("-"*30)
        
        with stats_lock:
            sorted_stats = dict(sorted(filter_stats.items(), key=lambda item: item[1], reverse=True))
            
        for reason, count in sorted_stats.items():
            print(f"❌ {reason:<25}: {count}")
        print("-"*30)
        print(f"入围数量: {len(results)}")
        
        if results: 
            return pd.DataFrame(results)
        else: 
            print("⚠️ 本次扫描未发现符合策略的标的。")
            return None

# ================= 5. 第二阶段：God Tier 资金博弈 =================

class AdvancedSelector:
    def get_moneyflow_bulk(self, ts_code_list):
        print(f"⚡ 正在批量拉取主力资金流 (Batch Size: {len(ts_code_list)})...")
        if not ts_code_list: return pd.DataFrame()
        
        chunk_size = 50
        all_dfs = []
        
        end_d = datetime.datetime.now().strftime('%Y%m%d')
        start_d = (datetime.datetime.now() - datetime.timedelta(days=15)).strftime('%Y%m%d')

        for i in range(0, len(ts_code_list), chunk_size):
            chunk = ts_code_list[i : i + chunk_size]
            codes_str = ",".join(chunk)
            try:
                df_chunk = pro.moneyflow(ts_code=codes_str, start_date=start_d, end_date=end_d)
                if df_chunk is not None and not df_chunk.empty:
                    all_dfs.append(df_chunk)
                time.sleep(0.2)
            except Exception as e:
                print(f"⚠️ 资金流批量获取失败: {e}")
        
        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()

    def score_stocks(self, candidates_df):
        print("\n>>> 💰 启动 God Tier 资金深度审计...")
        if candidates_df is None or candidates_df.empty: return None

        now_hour = datetime.datetime.now().hour
        is_intraday = (9 <= now_hour < 15)
        
        sector_flow_df = None
        try:
            target_date = datetime.datetime.now().strftime('%Y%m%d')
            if is_intraday:
                target_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
            
            for delta in range(5):
                d_str = (datetime.datetime.strptime(target_date, '%Y%m%d') - datetime.timedelta(days=delta)).strftime('%Y%m%d')
                try:
                    sector_flow_df = pro.moneyflow_industry(trade_date=d_str)
                    if not sector_flow_df.empty:
                        sector_flow_df['net_inflow_yi'] = sector_flow_df['net_amount'] / 100000000
                        print(f"✅ 板块资金流已就位 (日期: {d_str})")
                        break
                except: pass
        except: pass

        code_list = candidates_df['代码'].tolist()
        mf_dict = {}
        
        df_mf_bulk = self.get_moneyflow_bulk(code_list)
        
        if not df_mf_bulk.empty:
            for code, group in df_mf_bulk.groupby('ts_code'):
                total_inflow = group.head(5)['net_mf_amount'].sum()
                mf_dict[code] = total_inflow

        scored_results = []
        for _, row in tqdm(candidates_df.iterrows(), total=len(candidates_df), desc="多因子评分"):
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
                        score += 30; reasons.append(f"🔥行业爆买({round(net_yi,1)}亿)")
                        sector_is_hot = True
                    elif net_yi < -5.0: 
                        score -= 30; reasons.append(f"⚠️行业出逃")

            risk_penalty = False
            if risk_tags:
                if sector_is_hot:
                    reasons.append(f"🛡️风口豁免")
                else:
                    score -= 50
                    risk_penalty = True
                    reasons.append(f"❌形态风险")

            mv_yi = row['circ_mv'] / 10000 
            if 20 <= mv_yi <= 150:
                score += 20; reasons.append(f"💰黄金市值")
            elif mv_yi < 20:
                score += 10; reasons.append(f"微盘")
            
            to = row['Turnover']
            if 7 <= to <= 20:
                score += 20; reasons.append(f"⚡活跃")
            elif to > 25:
                score -= 10; reasons.append(f"过热({to}%)")

            if ts_code in mf_dict:
                net_wanyuan = mf_dict[ts_code]
                if net_wanyuan > 10000:
                    score += 50; reasons.append(f"🐳主力抢筹({int(net_wanyuan/100)}万)")
                elif net_wanyuan > 3000:
                    score += 20; reasons.append(f"机构吸纳")
                elif net_wanyuan < -5000:
                    score -= 30; reasons.append(f"主力出货")

            sugg = "⚪观察"
            if score >= 60: sugg = "⭐强烈推荐"
            elif score >= 30: sugg = "✅建议关注"
            if risk_penalty and score < 50: sugg = "⚠️谨慎"
            
            scored_results.append({
                '代码': row['代码'], '名称': row['名称'], '行业': row['行业'],
                '总分': score, '建议': sugg, 
                '现价': row['现价'], '涨幅': row['涨幅'], '换手': to,
                '主力资金(5日)': f"{int(mf_dict.get(ts_code,0)/100)}万",
                '核心理由': " ".join(reasons)
            })

        return pd.DataFrame(scored_results).sort_values(by='总分', ascending=False)

if __name__ == "__main__":
    print(f"========= Quant Master V7.2 Optimized ({datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}) =========")
    print(f"📋 优化后参数清单:")
    print(f"   爆量: {EXPLOSION_LOOKBACK_DAYS}日 涨幅{EXPLOSION_PCT_MIN}-{EXPLOSION_PCT_MAX}% 量比>{EXPLOSION_VOL_MULT}")
    print(f"   多头: {BULL_DAYS_THRESHOLD}/{BULL_DAYS_WINDOW}天 MA20>MA30×{MA20_MA30_TOLERANCE}")
    print(f"   斜率: MA10容差{MA10_SLOPE_TOLERANCE} MA30支撑>{MA30_SUPPORT_TOLERANCE}")
    print(f"   击球: MA10+{MAX_DIST_MA10_LOW*100}%~+{MAX_DIST_MA10*100}%  乖离<{MAX_DIST_MA10_MA20*100}%")
    print(f"   防追高: 20日涨幅<{MAX_20_DAYS_RISE*100}%")
    print(f"   量能模型: A股U型日内曲线")
    print(f"   退出(参考): TP1=+6% TP2=+8% SL=-5% 最长2天")
    print(f"=" * 65)
    
    st = TailEndStrategy()
    df_tech = st.run()
    
    if df_tech is not None and not df_tech.empty:
        print(f"\n技术初筛通过: {len(df_tech)} 只")
        sl = AdvancedSelector()
        final = sl.score_stocks(df_tech)
        
        print("\n" + "="*100)
        cols = ['代码','名称','行业','总分','建议','现价','涨幅','换手','核心理由']
        print(final[cols].to_markdown(index=False))
        print("="*100)
        
        fname = f"GodTier_Selection_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        final.to_csv(fname, index=False, encoding='utf_8_sig')
        print(f"\n✅ 结果已保存: {fname}")
    else:
        print("\n今日无符合条件的个股。")