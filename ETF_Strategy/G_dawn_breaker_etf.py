# ==============================================================================
# 系统代号: DawnBreaker Pro (破晓者) - 旗舰实战版
# 开发作者: Quant Master
# 核心架构: Tushare (历史筛选) + Sina Finance (实时快照) + 分批请求引擎
# 功能模块: 自动日历校准、超额跌幅计算、Moomoo批量导入CSV生成
# ==============================================================================

import tushare as ts
import pandas as pd
import datetime
import requests
import warnings
import re
import os
import csv

# 忽略不必要的警告
warnings.filterwarnings('ignore')

# ==========================================
# 1. 专用数据通道初始化 (Tushare 私有节点)
# ==========================================
try:
    pro = ts.pro_api('init_placeholder')
    pro._DataApi__token     = '4502206263062043105'
    pro._DataApi__http_url  = 'http://5k1a.xiximiao.com/dataapi'
    print("✅ [系统] 专用数据通道已激活。")
except Exception as e:
    print(f"❌ [系统] Tushare 初始化失败: {e}")
    exit()

# ==========================================
# 2. 实时行情引擎 (Sina Web API - 抗并发分批版)
# ==========================================
class SinaEngine:
    @staticmethod
    def get_realtime_data(ts_codes):
        """
        分批获取实时行情，防止 URL 过长导致 414 错误，并解决 GBK 编码问题
        """
        if not ts_codes: return {}
        
        results = {}
        headers = {'Referer': 'https://finance.sina.com.cn/'}
        
        # A. 获取大盘基准 (沪深300)
        try:
            bm_url = "http://hq.sinajs.cn/list=s_sh000300"
            res_bm = requests.get(bm_url, headers=headers, timeout=5)
            res_bm.encoding = 'gbk' 
            match = re.search(r'="(.+)"', res_bm.text)
            if match:
                bm_data = match.group(1).split(',')
                results['benchmark'] = {
                    'price': float(bm_data[1]),
                    'pct_chg': float(bm_data[3]) / 100.0
                }
        except Exception as e:
            print(f"❌ [实时] 基准抓取失败: {e}")
            return {}

        # B. 分批处理 ETF 标的池
        params = [f"{c.split('.')[1].lower()}{c.split('.')[0]}" for c in ts_codes]
        chunk_size = 80 # 关键安全阈值
        for i in range(0, len(params), chunk_size):
            chunk = params[i : i + chunk_size]
            chunk_codes = ts_codes[i : i + chunk_size]
            url = f"http://hq.sinajs.cn/list={','.join(chunk)}"
            try:
                res = requests.get(url, headers=headers, timeout=5)
                res.encoding = 'gbk'
                lines = res.text.strip().split('\n')
                for j, line in enumerate(lines):
                    match = re.search(r'="(.+)"', line)
                    if match and len(match.group(1)) > 10:
                        data = match.group(1).split(',')
                        open_p, pre_close = float(data[1]), float(data[2])
                        if open_p > 0:
                            results[chunk_codes[j]] = {
                                'open': open_p, 'pre_close': pre_close, 'gap': (open_p/pre_close - 1)
                            }
            except: continue
        return results

# ==========================================
# 3. 交易日历自动校准 (修复排序逻辑)
# ==========================================
def get_trading_dates():
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y%m%d')
    try:
        cal_df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=today_str)
        # 强制升序排列，解决 T(0) 和 T(-1) 颠倒的 Bug
        cal_df = cal_df.sort_values('cal_date', ascending=True).reset_index(drop=True)
        today_status = cal_df[cal_df['cal_date'] == today_str]['is_open'].values[-1]
        if today_status == 0:
            print(f"⏸️ [日历] 今日 ({today_str}) 为非交易日，系统休眠。")
            return None, None
        open_days = cal_df[cal_df['is_open'] == 1]['cal_date'].tolist()
        return open_days[-2], open_days[-1]
    except: return None, None

# ==========================================
# 4. 主策略执行逻辑
# ==========================================
def run_dawn_breaker():
    print(f"\n[INFO] 正在启动 DawnBreaker Pro 旗舰引擎...")
    
    t1_date, t0_date = get_trading_dates()
    if not t1_date: return
    print(f"📅 [日历] T-1: {t1_date} | T0: {t0_date}")

    # A. 静态筛选 (Tushare)
    fund_list = pro.fund_basic(market='E', status='L')
    df_t1 = pro.fund_daily(trade_date=t1_date)
    df_t1 = pd.merge(df_t1, fund_list[['ts_code', 'name']], on='ts_code', how='inner')
    watch_pool = df_t1[(df_t1['amount'] > 5000) & (df_t1['amount'] < 20000)]
    print(f"🔍 [观察] 锁定冷门标的 {len(watch_pool)} 只")

    # B. 动态捕获 (Sina API)
    realtime_results = SinaEngine.get_realtime_data(watch_pool['ts_code'].tolist())
    if 'benchmark' not in realtime_results: return
    bm_gap = realtime_results['benchmark']['pct_chg']
    print(f"📈 [基准] 沪深300实时开盘: {bm_gap*100:.2f}%")

    # C. 计算信号
    signals = []
    for code, data in realtime_results.items():
        if code == 'benchmark': continue
        excess_drop = data['gap'] - bm_gap
        if excess_drop < -0.015: # 阈值：超额低开 1.5%
            name = watch_pool[watch_pool['ts_code']==code]['name'].values[0]
            signals.append({'code': code, 'name': name, 'open': data['open'], 'excess': excess_drop})

# D. 输出并导出 CSV
    print(f"\n========== 🎯 狙击雷达战报 ==========")
    if not signals:
        print("🛡️ 今日无异常机会，保持空仓。")
    else:
        signals = sorted(signals, key=lambda x: x['excess'])[:5]
        csv_fn = f"Moomoo_DawnBreaker_{t0_date}.csv"
        
        with open(csv_fn, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            # 【修改点 1】表头去掉“建议价格”
            writer.writerow(['代码', '名称', '超额跌幅'])
            
            for s in signals:
                print(f"⚔️ {s['code']} ({s['name']}) | 超额跌幅: {s['excess']*100:.2f}%")
                
                # 【修改点 2】只提取纯数字代码 (Tushare格式为 '159189.SZ'，split('.') 后取第 0 个元素即 '159189')
                pure_code = s['code'].split('.')[0]
                
                # 【修改点 3】写入数据时，只写前三项
                writer.writerow([pure_code, s['name'], f"{s['excess']*100:.2f}%"])
                
        print(f"\n📁 CSV文件已生成: {os.path.abspath(csv_fn)}")
        print(f"💡 操作提示: 请直接导入此纯净版文件。")
    print("======================================")

if __name__ == "__main__":
    run_dawn_breaker()