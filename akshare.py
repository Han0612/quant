import akshare as ak
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
# 忽略烦人的 Pandas 警告
warnings.filterwarnings('ignore')

# 【关键网络配置】强制禁用系统代理，解决 AkShare 连接超时问题
os.environ['NO_PROXY'] = 'eastmoney.com'
os.environ['no_proxy'] = 'eastmoney.com'

# 1. 基础系统配置
MAX_WORKERS = 4
stats_lock = threading.Lock() # 线程锁，防止计数器数据丢失

# 2. 【核心策略参数】 (维持 V4.5 黄金逻辑)
MAX_DIST_MA10 = 0.05        # 收盘价距离 MA10 允许偏离 2.5%
MAX_DIST_MA10_MA20 = 0.12     # 均线发散程度不超过 12%
EXPLOSION_LOOKBACK_DAYS = 13  # 回溯 13 天寻找大阳线
MAX_20_DAYS_RISE = 0.60       # 20天累计涨幅不超过 60%

# 3. 【动态风控参数】 (用于打标签，不直接剔除)
AMP_THRESHOLD = 0.06          # 振幅警戒线 6%
TURNOVER_RATIO_LIMIT = 1.5    # 相对换手倍数限制 (今日 / 5日均值)

# 全局计数器 (用于统计拦截情况)
filter_stats = {
    "1_trend_error": 0,      # 趋势坏了
    "2_multi_head_error": 0, # 均线非多头
    "3_ma_dist_error": 0,    # 乖离率过大
    "4_candle_error": 0,     # 未收阴
    "5_vol_error": 0,        # 未缩量
    "6_price_pos_error": 0,  # 没踩到 MA10
    "7_high_pos_error": 0,   # 累计涨幅过高
    "8_explode_error": 0,    # 无主力大阳线
    "99_unknown_error": 0    # 未知错误
}

# ================= 1. 第一阶段：技术面筛选策略 =================

class TailEndStrategy:
    def __init__(self):
        self.lookback_days = 60  
        
    def get_market_snapshot(self):
        max_retries = 5
        for i in range(max_retries):
            try:
                print(f"正在拉取全市场实时行情 (尝试 {i+1}/{max_retries})...")
                df = ak.stock_zh_a_spot_em()
                df = df.rename(columns={
                    '代码': 'symbol', '名称': 'name', '最新价': 'close', 
                    '最高': 'high', '最低': 'low', '今开': 'open', 
                    '昨收': 'pre_close', '成交量': 'volume', '成交额': 'amount',
                    '涨跌幅': 'pct_chg', '换手率': 'turnover'
                })
                # 【稳定性修复】清洗脏数据，防止后续计算报错
                numeric_cols = ['close', 'high', 'low', 'open', 'pre_close', 'volume', 'pct_chg', 'turnover']
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
                print("行情拉取成功！")
                return df
            except Exception as e:
                time.sleep(3)
                if i == max_retries - 1: raise e

    def filter_universe(self, df_snapshot):
        valid_symbols = []
        for code in df_snapshot['symbol']:
            if code.startswith(('60', '00')):
                valid_symbols.append(code)
        df_filtered = df_snapshot[df_snapshot['symbol'].isin(valid_symbols)]
        df_filtered = df_filtered[df_filtered['volume'] > 0] 
        return df_filtered

    def check_individual_stock(self, symbol, snapshot_row):
        try:
            # 1. 获取历史数据
            df_hist = None
            for retry in range(3):
                try:
                    df_hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", adjust="qfq")
                    if df_hist is not None and not df_hist.empty: break
                except: time.sleep(0.1)
            
            if df_hist is None or len(df_hist) < 40: return None
            
            # 数据清洗
            if '换手率' not in df_hist.columns: df_hist['换手率'] = np.nan
            df_hist = df_hist[['日期', '开盘', '收盘', '最高', '最低', '成交量', '涨跌幅', '换手率']]
            df_hist.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'pct_chg', 'turnover']
            df_hist['date'] = pd.to_datetime(df_hist['date']).dt.strftime('%Y-%m-%d')
            
            # --- 智能时间对齐 (防止周末或收盘后数据缺失) ---
            last_hist_date = df_hist.iloc[-1]['date']
            current_date = datetime.datetime.now()
            current_date_str = current_date.strftime("%Y-%m-%d")
            is_weekend = current_date.weekday() >= 5
            is_intraday = False
            
            if (last_hist_date != current_date_str) and (not is_weekend):
                is_intraday = True
                new_row = {
                    'date': current_date_str,
                    'open': snapshot_row['open'],
                    'close': snapshot_row['close'],
                    'high': snapshot_row['high'],
                    'low': snapshot_row['low'],
                    'volume': snapshot_row['volume'],
                    'pct_chg': snapshot_row['pct_chg'],
                    'turnover': snapshot_row['turnover']
                }
                df_hist = pd.concat([df_hist, pd.DataFrame([new_row])], ignore_index=True)
            
            df_hist = df_hist.drop_duplicates(subset=['date'], keep='last')
            cols = ['close', 'open', 'high', 'low', 'volume', 'pct_chg', 'turnover']
            df_hist[cols] = df_hist[cols].apply(pd.to_numeric, errors='coerce')
            
            # --- 指标计算 ---
            df_hist['MA10'] = df_hist['close'].rolling(window=10).mean()
            df_hist['MA20'] = df_hist['close'].rolling(window=20).mean()
            df_hist['MA30'] = df_hist['close'].rolling(window=30).mean() # 【修复】确保 MA30 存在
            df_hist['MA60'] = df_hist['close'].rolling(window=60).mean()
            df_hist['Vol_MA5'] = df_hist['volume'].rolling(window=5).mean()
            
            today = df_hist.iloc[-1]
            yesterday = df_hist.iloc[-2]
            
            fail_reason = None
            tags = [] # 风险标签

            # ================= V4.9 Pro: 动态风控 (只打标，不拦截) =================
            
            # 预估全天成交量
            current_vol = today['volume']
            projected_vol = current_vol
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
                    projected_vol = current_vol * (240 / trade_minutes)

            # --- 动态规则 A: 振幅 + 量能联动 ---
            pre_close = float(snapshot_row['pre_close'])
            amplitude = 0
            if pre_close > 0:
                amplitude = (today['high'] - today['low']) / pre_close
            
            if amplitude > AMP_THRESHOLD: 
                if projected_vol >= yesterday['volume']:
                    # 高振幅 + 放量 = 风险
                    tags.append("RISK_高振幅放量")
                else:
                    pass # 缩量大震荡 = 观察

            # --- 动态规则 B: 相对换手率风控 ---
            try:
                hist_turnover = df_hist['turnover'].iloc[-6:-1] # 前5天
                avg_turnover_5 = hist_turnover.mean()
                current_turnover = float(snapshot_row['turnover'])
                
                if avg_turnover_5 > 0:
                    if current_turnover > (avg_turnover_5 * TURNOVER_RATIO_LIMIT):
                        tags.append("RISK_换手异常")
            except: pass

            # ========================================================
            
            # --- 基础技术铁律 (严格拦截) ---
            
            # 1. 累计涨幅保护 (防止接盘)
            if not fail_reason:
                try:
                    price_20_days_ago = df_hist.iloc[-20]['close']
                    cumulative_rise = (today['close'] - price_20_days_ago) / price_20_days_ago
                    if cumulative_rise > MAX_20_DAYS_RISE:
                        fail_reason = "7_high_pos_error"
                except: pass

            # 2. 趋势向上 (MA10)
            if not fail_reason:
                if not (today['MA10'] > yesterday['MA10']):
                    fail_reason = "1_trend_error"

            # 3. 多头排列 (MA10 > MA20 > MA30)
            if not fail_reason:
                history_check = df_hist.iloc[-7:] 
                check_10_gt_20 = (history_check['MA10'] > history_check['MA20']).all()
                
                # 【修复】增加 MA30 存在的判断
                check_20_gt_30 = True
                if 'MA30' in history_check.columns:
                    check_20_gt_30 = (history_check['MA20'] > history_check['MA30']).all()
                
                check_price_safe = (history_check['close'] > history_check['MA20']).all()
                
                if not (check_10_gt_20 and check_20_gt_30 and check_price_safe):
                    fail_reason = "2_multi_head_error"
            
            # 4. 乖离率控制
            if not fail_reason:
                if today['MA20'] == 0: 
                    fail_reason = "3_ma_dist_error"
                else:
                    dist_ma10_ma20 = (today['MA10'] - today['MA20']) / today['MA20']
                    if dist_ma10_ma20 > MAX_DIST_MA10_MA20 or dist_ma10_ma20 < 0: 
                        fail_reason = "3_ma_dist_error"

            # 5. 必须收阴 (回调)
            if not fail_reason:
                if not (today['close'] < today['open']):
                    fail_reason = "4_candle_error"

            # 6. 必须缩量 (策略灵魂)
            # Quant Master 注：虽然您希望放行"高振幅放量"，但本策略是"缩量回调"策略。
            # 如果这里不拦截缩量，那整个策略逻辑就变成了"追涨"。
            # 为了平衡，我们依然要求整体是缩量的。
            # 只有极少数"缩量但换手高"的情况会被上面的 RISK_换手异常 捕获。
            if not fail_reason:
                if projected_vol >= yesterday['volume']:
                    fail_reason = "5_vol_error"

            # 7. 回踩到位
            if not fail_reason:
                if today['MA10'] == 0: 
                    fail_reason = "6_price_pos_error"
                else:
                    dist_ma10 = (today['close'] - today['MA10']) / today['MA10']
                    if dist_ma10 >= MAX_DIST_MA10 or dist_ma10 < 0: 
                        fail_reason = "6_price_pos_error"
            
            # 8. 曾有大阳线
            if not fail_reason:
                window_df = df_hist.iloc[-EXPLOSION_LOOKBACK_DAYS:-1]
                found_explosion = False
                for idx, row in window_df.iterrows():
                    if 8 <= row['pct_chg'] <= 20:
                        if row['close'] > row['open'] and row['volume'] > (row['Vol_MA5'] * 1.5): 
                            found_explosion = True
                            break
                if not found_explosion:
                    fail_reason = "8_explode_error"

            # --- 统一拦截 ---
            if fail_reason:
                with stats_lock:
                    filter_stats[fail_reason] += 1
                return None

            # --- 补充软性标记 ---
            check_shadow_df = df_hist.iloc[-4:-1]
            for _, row in check_shadow_df.iterrows():
                upper_shadow_len = row['high'] - max(row['open'], row['close'])
                if row['close'] > 0:
                    shadow_pct = upper_shadow_len / row['close']
                    if shadow_pct > 0.035: 
                        tags.append("RISK_SHADOW")
                        break 

            check_trap_df = df_hist.iloc[-6:-2]
            for _, row in check_trap_df.iterrows():
                if row['pct_chg'] < -3.0:
                    vol_ma5 = row['Vol_MA5'] if not pd.isna(row['Vol_MA5']) else 0
                    if vol_ma5 > 0 and row['volume'] > (vol_ma5 * 1.1):
                        tags.append("RISK_TRAP")
                        break

            return {
                '代码': symbol,
                '名称': snapshot_row['name'],
                '现价': today['close'],
                '涨幅': today['pct_chg'],
                '振幅': round(amplitude * 100, 2),
                '相对换手': round(current_turnover / avg_turnover_5, 2) if avg_turnover_5 > 0 else 0,
                'MA10': round(today['MA10'], 2),
                'RiskTags': tags  
            }

        except Exception as e:
            # 捕获未知错误
            with stats_lock:
                filter_stats["99_unknown_error"] += 1
            return None

    def run(self):
        try:
            df_snapshot = self.get_market_snapshot()
        except Exception as e:
            return None

        df_target = self.filter_universe(df_snapshot)
        print(f"初步筛选进入计算池: {len(df_target)}")
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_stock = {
                executor.submit(self.check_individual_stock, row['symbol'], row): row['symbol'] 
                for _, row in df_target.iterrows()
            }
            
            for future in tqdm(as_completed(future_to_stock), total=len(df_target), desc="技术面扫描"):
                res = future.result()
                if res: results.append(res)
        
        print("\n--- 技术筛选统计 ---")
        total_blocked = sum(filter_stats.values())
        print(filter_stats)
        print(f"总计拦截: {total_blocked} / 总样本: {len(df_target)}")

        if results:
            return pd.DataFrame(results)
        else:
            print("\n❌ 今日无符合基础技术形态的个股。")
            return None

# ================= 2. 第二阶段：资金博弈与评分器 (V4.9.1 强力修复版) =================

class AdvancedSelector:
    def __init__(self):
        self.fund_flow_df = None
    
    def _get_sector_fund_flow(self):
        try:
            print("💰 正在拉取板块资金流向 (双模式: 今日/5日)...")
            # 尝试获取行业资金流
            df_flow = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
            
            if df_flow is None or df_flow.empty:
                print("⚠️ 警告: '今日'资金流接口无数据，尝试拉取'5日'数据...")
                df_flow = ak.stock_sector_fund_flow_rank(indicator="5日", sector_type="行业资金流")
                # 5日接口列名不同，需要重命名对齐
                if df_flow is not None:
                    df_flow = df_flow.rename(columns={'5日主力净流入': '今日主力净流入', '5日主力净流入占比': '今日主力净流入占比', '5日涨跌幅': '今日涨跌幅'})

            if df_flow is None or df_flow.empty:
                print("❌ 严重错误: 无法获取任何板块资金数据。")
                return None
            
            # 打印列名以便调试
            # print(f"DEBUG: 获取到的列名: {df_flow.columns.tolist()}")
            
            # --- 强壮的单位清洗逻辑 ---
            def parse_money_force(x):
                # 1. 如果已经是数字，直接返回
                if isinstance(x, (int, float)):
                    return float(x)
                
                # 2. 如果是字符串，处理单位
                x = str(x).strip()
                if x == '-' or x == '': return 0
                
                multi = 1.0
                if '亿' in x: 
                    multi = 100000000.0
                    x = x.replace('亿', '')
                elif '万' in x: 
                    multi = 10000.0
                    x = x.replace('万', '')
                
                # 提取数字
                try:
                    return float(x) * multi
                except:
                    return 0

            # 统一列名处理 (防止接口变动)
            # 查找包含 '净流入' 的列
            inflow_col = [c for c in df_flow.columns if '净流入' in c and '占比' not in c][0]
            ratio_col = [c for c in df_flow.columns if '占比' in c][0]
            chg_col = [c for c in df_flow.columns if '涨跌幅' in c][0]

            df_flow['net_inflow'] = df_flow[inflow_col].apply(parse_money_force)
            df_flow['inflow_ratio'] = pd.to_numeric(df_flow[ratio_col].astype(str).str.replace('%',''), errors='coerce')
            df_flow['sector_chg'] = pd.to_numeric(df_flow[chg_col].astype(str).str.replace('%',''), errors='coerce')
            
            # 【调试信息】打印前3名，证明数据拿到了
            df_sorted = df_flow.sort_values(by='net_inflow', ascending=False)
            print(f"🔍 资金流数据就位! 榜首板块: {df_sorted.iloc[0]['名称']} ({df_sorted.iloc[0]['net_inflow']/1e8:.2f}亿)")
            
            return df_flow
        except Exception as e: 
            print(f"❌ 板块资金获取抛出异常: {e}")
            return None

    def _get_stock_details(self, symbol):
        try:
            info_df = ak.stock_individual_info_em(symbol=symbol)
            if info_df.empty: return None
            industry = info_df[info_df['item'] == '行业']['value'].values[0]
            circ_mv_str = info_df[info_df['item'] == '流通市值']['value'].values[0]
            circ_mv = float(circ_mv_str) if isinstance(circ_mv_str, (int, float)) else 0
            
            quote_df = ak.stock_zh_a_spot_em()
            stock_row = quote_df[quote_df['代码'] == symbol]
            turnover = float(stock_row['换手率'].values[0]) if not stock_row.empty else 0
            
            return {'industry': industry, 'circ_mv': circ_mv, 'turnover': turnover}
        except: return None

    def score_stocks(self, candidates_df):
        print("\n>>> 🚀 启动第二阶段：资金豁免博弈评分...")
        sector_flow_df = self._get_sector_fund_flow()
        
        scored_results = []
        for _, row in tqdm(candidates_df.iterrows(), total=len(candidates_df), desc="博弈分析"):
            symbol = row['代码']
            name = row['名称']
            risk_tags = row['RiskTags']
            
            time.sleep(0.1)
            details = self._get_stock_details(symbol)
            if not details: continue
            
            score = 0
            reasons = []
            
            # --- 1. 板块资金博弈 (暴力匹配) ---
            ind_name = details['industry']
            net_inflow_yi = 0
            sector_is_hot = False
            
            if sector_flow_df is not None:
                # 暴力匹配逻辑：
                # 1. 尝试直接等于
                match = sector_flow_df[sector_flow_df['名称'] == ind_name]
                
                # 2. 尝试包含 (个股行业 in 板块名 OR 板块名 in 个股行业)
                if match.empty:
                    # 将df转为字典遍历，确保匹配
                    for idx, s_row in sector_flow_df.iterrows():
                        s_name = s_row['名称']
                        # 核心：双向包含检测，且去除"行业"干扰
                        clean_ind = ind_name.replace("行业", "")
                        clean_sec = s_name.replace("行业", "")
                        if clean_ind in clean_sec or clean_sec in clean_ind:
                            match = pd.DataFrame([s_row])
                            break
                
                if not match.empty:
                    sector_data = match.iloc[0]
                    net_inflow_yi = sector_data['net_inflow'] / 100000000.0
                    sector_chg = sector_data['sector_chg']
                    
                    # 评分标准
                    if net_inflow_yi > 3.0: 
                        sector_is_hot = True
                        score += 50
                        reasons.append(f"🔥资金爆买({round(net_inflow_yi,1)}亿)")
                    elif net_inflow_yi > 1.0 and sector_chg < 1.0:
                        sector_is_hot = True
                        score += 30
                        reasons.append(f"⚡主力潜伏({round(net_inflow_yi,1)}亿)")
                    elif net_inflow_yi < -1.0:
                        score -= 50
                        reasons.append(f"⚠️资金出逃({round(net_inflow_yi,1)}亿)")
            
            # --- 2. 风险豁免逻辑 ---
            risk_penalty = False
            if risk_tags:
                if sector_is_hot:
                    reasons.append(f"🛡️热点豁免({','.join(risk_tags)})")
                else:
                    score -= 100
                    risk_penalty = True
                    reasons.append(f"❌形态恶劣({','.join(risk_tags)})")
                    
            # --- 3. 基础分 ---
            mv_yi = details['circ_mv'] / 100000000
            if 30 <= mv_yi <= 120: 
                score += 20
                reasons.append(f"💰黄金市值({round(mv_yi,1)}亿)")
            elif mv_yi < 30: 
                score += 10
                reasons.append(f"微盘股({round(mv_yi,1)}亿)")
            elif mv_yi > 120:
                score += 5
                reasons.append(f"大盘股({round(mv_yi,1)}亿)")
            
            to = details['turnover']
            if 5 <= to <= 15: 
                score += 20
                reasons.append(f"⚡活跃换手({to}%)")
            elif 3 <= to < 5: 
                score += 10
                reasons.append(f"温和换手({to}%)")
            elif to > 20: 
                score -= 5 
                reasons.append(f"过热({to}%)")
            
            # --- 4. 生成建议 ---
            suggestion = ""
            if score >= 50: suggestion = "⭐强烈推荐"
            elif score > 0: suggestion = "✅建议关注"
            elif risk_penalty: suggestion = "⚠️需确认板块风口" 
            else: suggestion = "⚪观察"

            scored_results.append({
                '代码': symbol,
                '名称': name,
                '总分': score,
                '现价': row['现价'],
                '建议': suggestion,
                '板块': ind_name, 
                '相对换手': row['相对换手'],
                '板块净流(亿)': round(net_inflow_yi, 2),
                '核心理由': " ".join(reasons)
            })

        df_scored = pd.DataFrame(scored_results)
        if not df_scored.empty:
            df_scored = df_scored.sort_values(by='总分', ascending=False)
        return df_scored
    
# ================= 主程序入口 =================

if __name__ == "__main__":
    print(f"========= Quant Master V4.9 (Pro Plus - Soft Filter) ({datetime.datetime.now().strftime('%A %H:%M')}) =========")
    
    strategy = TailEndStrategy()
    df_tech = strategy.run()
    
    if df_tech is not None and not df_tech.empty:
        print(f"技术初筛通过: {len(df_tech)} 只")
        
        # 备份中间结果
        tech_filename = f"tech_candidates_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        df_tech.to_csv(tech_filename, index=False, encoding='utf_8_sig')
        print(f"✅ 备份成功: 技术面初筛结果已保存至 {tech_filename}")
        
        # 第二阶段评分
        selector = AdvancedSelector()
        final_df = selector.score_stocks(df_tech)
        
        if final_df is not None and not final_df.empty:
            print("\n" + "="*100)
            print(f"💎 动态风控优选池 (含软性风险标记) 💎")
            print("="*100)
            # 打印表格
            print(final_df[['代码','名称','总分','建议','相对换手','板块','板块净流(亿)','核心理由']].to_markdown(index=False))
            
            # 保存最终结果
            csv_name = f"V4_9_Candidates_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
            final_df.to_csv(csv_name, index=False, encoding='utf_8_sig')
            print(f"\n✅ 完整列表已保存: {csv_name}")
            print("Quant Master 提示: 请重点检查【RISK_...】标签，如板块资金流为负，请果断放弃该股。")
        else:
            print("评分后无标的存活。")
    else:
        print("今日全市场无基础符合条件的个股。")