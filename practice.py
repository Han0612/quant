import akshare as ak
import pandas as pd
import os
import time
import random
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# === 配置 ===
SAVE_DIR = "stock_data"  # 数据保存目录
START_DATE = "20240601"  # 下载最近半年数据即可满足策略需求
END_DATE = "20260101"

def download_single_stock(symbol, name):
    try:
        # 随机延迟防封
        time.sleep(random.uniform(0.1, 0.3))
        
        # 获取日线
        df = ak.stock_zh_a_hist(symbol=symbol, start_date=START_DATE, end_date=END_DATE, adjust="qfq")
        
        if df is None or df.empty:
            return False
        
        # 保存为 CSV
        file_path = os.path.join(SAVE_DIR, f"{symbol}.csv")
        df.to_csv(file_path, index=False, encoding='utf_8_sig')
        return True
    except:
        return False

def run_downloader():
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)
    
    print("正在获取全市场股票列表...")
    try:
        stock_info = ak.stock_zh_a_spot_em()
    except:
        print("❌ 无法获取股票列表，请检查网络！")
        return

    # 只下载主板 (60, 00)
    pool_df = stock_info[stock_info['代码'].str.startswith(('60', '00'))]
    all_symbols = pool_df['代码'].tolist()
    all_names = dict(zip(pool_df['代码'], pool_df['名称']))
    
    print(f"共找到 {len(all_symbols)} 只主板股票，开始下载...")
    print(f"数据将保存在当前目录下的 {SAVE_DIR} 文件夹中。")
    
    success_count = 0
    
    # 使用多线程下载 (建议在国内网络环境下运行)
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_stock = {
            executor.submit(download_single_stock, symbol, all_names.get(symbol)): symbol 
            for symbol in all_symbols
        }
        
        for future in tqdm(as_completed(future_to_stock), total=len(all_symbols)):
            if future.result():
                success_count += 1
                
    print(f"\n下载完成！成功: {success_count}/{len(all_symbols)}")
    print("请将 'stock_data' 文件夹复制到您的回测环境中。")

if __name__ == "__main__":
    run_downloader()