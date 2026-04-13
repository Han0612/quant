# -*- coding: utf-8 -*-
# ST_CLIENT.py - Tushare API 客户端封装
# 支持所有 ST_SERVER.py 后台服务接口

import requests
import pandas as pd
from typing import Optional, Dict, Any, List


class StockToday:
    """Tushare API 客户端"""

    def __init__(self, base_url: str = "https://tushare.citydata.club/", token: str = "test"):
        """
        初始化客户端

        Args:
            base_url: 后台服务地址
            token: API Token
        """
        self.url = base_url
        self.TOKEN = 4502247663219033923

    def _post(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """发送 POST 请求"""
        if params is None:
            params = {}
        params['TOKEN'] = self.TOKEN
        try:
            resp = requests.post(f"{self.url}{endpoint}", data=params, timeout=30)
            if resp.status_code != 200:
                return {"error": f"HTTP {resp.status_code}", "detail": resp.text[:200]}
            if not resp.text.strip():
                return {"error": "Empty response from server"}
            return resp.json()
        except Exception as e:
            return {"error": str(e)}



    # ==================== 2.1 基础数据 ====================

    def stock_basic(self, exchange: str = "", list_status: str = "L", fields: str = "",
                    ts_code: str = "", market: str = "") -> Any:
        """
        股票基本信息
        """
        params = {"exchange": exchange, "list_status": list_status, "fields": fields}
        if ts_code:
            params["ts_code"] = ts_code
        if market:
            params["market"] = market
        return self._post("/stock_basic", params)

    def stk_premarket(self, ts_code: str = "", trade_date: str = "",
                      start_date: str = "", end_date: str = "") -> Any:
        """新股上市"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/stk_premarket", params)

    def trade_cal(self, exchange: str = "SSE", start_date: str = "", end_date: str = "") -> Any:
        """
        交易日历
        """
        params = {"exchange": exchange}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/trade_cal", params)
        
    def stock_st(self,trade_date: str = "") -> Any:
        """
        ST股票列表
        """
        params = {"trade_date": trade_date}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/stock_st", params)
    
    def st(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: str = "") -> Any:
        """
        ST股票列表
        """
        params = {"trade_date": trade_date}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/stock_st", params)

    def namechange(self, ts_code: str = "", start_date: str = "", end_date: str = "") -> Any:
        """股票名称变更"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/namechange", params)

    def hs_const(self, hs_type: str = "1", is_new: str = "1") -> Any:
        """沪深股通成分股"""
        return self._post("/hs_const", {"hs_type": hs_type, "is_new": is_new})

    def stock_company(self, ts_code: str = "", exchange: str = "") -> Any:
        """上市公司信息"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if exchange:
            params["exchange"] = exchange
        return self._post("/stock_company", params)

    def stk_managers(self, ts_code: str, start_date: str = "", end_date: str = "",
                     ann_date: str = "") -> Any:
        """公司管理层"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if ann_date:
            params["ann_date"] = ann_date
        return self._post("/stk_managers", params)

    def stk_rewards(self, ts_code: str) -> Any:
        """高管薪酬"""
        return self._post("/stk_rewards", {"ts_code": ts_code})

    def new_share(self, ts_code: str = "", start_date: str = "", end_date: str = "") -> Any:
        """新股上市"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/new_share", params)

    def bak_basic(self, exchange: str = "", ts_code: str = "") -> Any:
        """备用基础数据"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if exchange:
            params["exchange"] = exchange
        return self._post("/bak_basic", params)

    # ==================== 2.2 行情数据 ====================

    def daily(self, ts_code: str = "", trade_date: str = "",
              start_date: str = "", end_date: str = "") -> Any:
        """
        日线行情
        """
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/daily", params)

    def weekly(self, ts_code: str = "", trade_date: str = "",
               start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """周线行情"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/weekly", params)

    def monthly(self, ts_code: str = "", trade_date: str = "",
                start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """月线行情"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/monthly", params)

    def pro_bar(self, ts_code: str, start_date: str = "", end_date: str = "",
                asset: str = "E", adj: str = "qfq", freq: str = "D",
                ma: str = "", factors: str = "", adjfactor: str = "") -> Any:
        """
        行情数据（支持复权/定时务）
        """
        params = {
            "ts_code": ts_code,
            "asset": asset,
            "adj": adj,
            "freq": freq
        }
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if ma:
            params["ma"] = ma
        if factors:
            params["factors"] = factors
        if adjfactor:
            params["adjfactor"] = adjfactor
        return self._post("/pro_bar", params)

    def adj_factor(self, ts_code: str = "", trade_date: str = "",
                   start_date: str = "", end_date: str = "") -> Any:
        """复权因子"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/adj_factor", params)

    def daily_basic(self, ts_code: str = "", trade_date: str = "",
                    start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """每日指标"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/daily_basic", params)

    def stk_limit(self, ts_code: str = "", trade_date: str = "",
                  start_date: str = "", end_date: str = "") -> Any:
        """涨跌停"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/stk_limit", params)

    def suspend_d(self, ts_code: str = "", trade_date: str = "",
                  start_date: str = "", end_date: str = "", suspend_type: str = "1") -> Any:
        """停复牌信息"""
        params = {"suspend_type": suspend_type}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/suspend_d", params)

    def hsgt_top10(self, trade_date: str = "", start_date: str = "",
                   end_date: str = "", market_type: str = "") -> Any:
        """沪深股通前十成交"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if market_type:
            params["market_type"] = market_type
        return self._post("/hsgt_top10", params)

    def ggt_top10(self, trade_date: str = "", ts_code: str = "",
                  start_date: str = "", end_date: str = "") -> Any:
        """广港通前十成交"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/ggt_top10", params)

    def ggt_daily(self, trade_date: str = "",
                  start_date: str = "", end_date: str = "") -> Any:
        """广港通每日成交"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/ggt_daily", params)

    def ggt_monthly(self, trade_date: str = "",
                    start_date: str = "", end_date: str = "") -> Any:
        """广港通每月成交"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/ggt_monthly", params)

    def bak_daily(self, ts_code: str = "", trade_date: str = "",
                  start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """备用每日行情"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/bak_daily", params)

    # ==================== 2.3 财务数据 ====================

    def income(self, ts_code: str, start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """利润表"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/income", params)

    def balancesheet(self, ts_code: str, start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """资产负债表"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/balancesheet", params)

    def cashflow(self, ts_code: str, start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """现金流量表"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/cashflow", params)

    def forecast(self, ann_date: str = "", fields: str = "") -> Any:
        """业绩预告"""
        params = {}
        if ann_date:
            params["ann_date"] = ann_date
        if fields:
            params["fields"] = fields
        return self._post("/forecast", params)

    def express(self, ts_code: str = "", start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """业绩快报"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/express", params)

    def dividend(self, ts_code: str, fields: str = "") -> Any:
        """分红送股"""
        params = {"ts_code": ts_code}
        if fields:
            params["fields"] = fields
        return self._post("/dividend", params)

    def fina_indicator(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """财务指标"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/fina_indicator", params)

    def fina_audit(self, ts_code: str = "", start_date: str = "", end_date: str = "") -> Any:
        """财务审计意见"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/fina_audit", params)

    def fina_mainbz(self, ts_code: str = "", type: str = "") -> Any:
        """主营业务"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if type:
            params["type"] = type
        return self._post("/fina_mainbz", params)

    def disclosure_date(self, end_date: str = "") -> Any:
        """披露日期"""
        params = {}
        if end_date:
            params["end_date"] = end_date
        return self._post("/disclosure_date", params)

    # ==================== 2.4 参考数据 ====================

    def top10_holders(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """十大股东"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/top10_holders", params)

    def top10_floatholders(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """十大流通股东"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/top10_floatholders", params)

    def pledge_stat(self, ts_code: str = "") -> Any:
        """股权质押统计"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/pledge_stat", params)

    def pledge_detail(self, ts_code: str = "") -> Any:
        """股权质押明细"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/pledge_detail", params)

    def repurchase(self, ann_date: str = "", start_date: str = "", end_date: str = "") -> Any:
        """股份回购"""
        params = {}
        if ann_date:
            params["ann_date"] = ann_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/repurchase", params)

    def concept(self) -> Any:
        """概念列表"""
        return self._post("/concept")

    def concept_detail(self, id: str = "", ts_code: str = "") -> Any:
        """概念详情"""
        params = {}
        if id:
            params["id"] = id
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/concept_detail", params)

    def share_float(self, ann_date: str = "") -> Any:
        """流通股本"""
        params = {}
        if ann_date:
            params["ann_date"] = ann_date
        return self._post("/share_float", params)

    def block_trade(self, trade_date: str = "") -> Any:
        """大宗交易"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/block_trade", params)

    def stk_holdernumber(self, ts_code: str = "", start_date: str = "", end_date: str = "") -> Any:
        """股东户数"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/stk_holdernumber", params)

    def stk_holdertrade(self, ts_code: str = "", ann_date: str = "", trade_type: str = "") -> Any:
        """股东增减持"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if ann_date:
            params["ann_date"] = ann_date
        if trade_type:
            params["trade_type"] = trade_type
        return self._post("/stk_holdertrade", params)

    # ==================== 2.5 特色数据 ====================

    def report_rc(self, ts_code: str = "", report_date: str = "") -> Any:
        """研报"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if report_date:
            params["report_date"] = report_date
        return self._post("/report_rc", params)

    def cyq_perf(self, ts_code: str = "", start_date: str = "", end_date: str = "") -> Any:
        """筹码活跃度"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/cyq_perf", params)

    def cyq_chips(self, ts_code: str = "", start_date: str = "", end_date: str = "") -> Any:
        """筹码分布"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/cyq_chips", params)

    def stk_factor(self, ts_code: str, start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """股票因子"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/stk_factor", params)

    def stk_factor_pro(self, ts_code: str = "", start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """股票因子(专业版)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/stk_factor_pro", params)

    def ccass_hold(self, ts_code: str = "") -> Any:
        """中央结算持股"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/ccass_hold", params)

    def ccass_hold_detail(self, ts_code: str = "", trade_date: str = "", fields: str = "") -> Any:
        """中央结算持股明细"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if fields:
            params["fields"] = fields
        return self._post("/ccass_hold_detail", params)

    def hk_hold(self, ts_code: str = "", exchange: str = "") -> Any:
        """港股持股"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if exchange:
            params["exchange"] = exchange
        return self._post("/hk_hold", params)

    def stk_auction_o(self, ts_code: str = "", trade_date: str = "",
                      start_date: str = "", end_date: str = "") -> Any:
        """集合竞价"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/stk_auction_o", params)

    def stk_auction_c(self, ts_code: str = "", trade_date: str = "",
                      start_date: str = "", end_date: str = "") -> Any:
        """盘后定价"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/stk_auction_c", params)

    def stk_nineturn(self, ts_code: str = "", trade_date: str = "",
                     start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """九转序列"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/stk_nineturn", params)

    def stk_surv(self, ts_code: str = "", trade_date: str = "", fields: str = "") -> Any:
        """舆情监控"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if fields:
            params["fields"] = fields
        return self._post("/stk_surv", params)

    def broker_recommend(self, month: str = "") -> Any:
        """券商研报推荐"""
        params = {}
        if month:
            params["month"] = month
        return self._post("/broker_recommend", params)

    # ==================== 2.6 两融数据 ====================

    def margin(self, trade_date: str = "") -> Any:
        """融资融券"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/margin", params)

    def margin_detail(self, trade_date: str = "") -> Any:
        """融资融券明细"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/margin_detail", params)

    def margin_secs(self, trade_date: str = "", exchange: str = "") -> Any:
        """融资融券证券"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if exchange:
            params["exchange"] = exchange
        return self._post("/margin_secs", params)

    def slb_sec(self, trade_date: str = "") -> Any:
        """融券余量"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/slb_sec", params)

    def slb_len(self, trade_date: str = "", start_date: str = "", end_date: str = "") -> Any:
        """融资期限"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/slb_len", params)

    def slb_sec_detail(self, trade_date: str = "") -> Any:
        """融券余量明细"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/slb_sec_detail", params)

    def slb_len_mm(self, trade_date: str = "") -> Any:
        """融资期限明细"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/slb_len_mm", params)

    # ==================== 2.7 资金流向 ====================

    def moneyflow(self, ts_code: str = "", trade_date: str = "",
                  start_date: str = "", end_date: str = "") -> Any:
        """资金流向"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/moneyflow", params)

    def moneyflow_ths(self, ts_code: str = "", trade_date: str = "",
                      start_date: str = "", end_date: str = "") -> Any:
        """资金流向(同花顺)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/moneyflow_ths", params)

    def moneyflow_cnt_ths(self, trade_date: str = "", ts_code: str = "") -> Any:
        """资金流向分类(同花顺)"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/moneyflow_cnt_ths", params)

    def moneyflow_dc(self, ts_code: str = "", trade_date: str = "",
                     start_date: str = "", end_date: str = "") -> Any:
        """资金流向(东方财富)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/moneyflow_dc", params)

    def moneyflow_ind_ths(self, ts_code: str = "", trade_date: str = "",
                          start_date: str = "", end_date: str = "") -> Any:
        """行业资金流向(同花顺)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/moneyflow_ind_ths", params)

    def moneyflow_ind_dc(self, ts_code: str = "", trade_date: str = "",
                         start_date: str = "", end_date: str = "") -> Any:
        """行业资金流向(东方财富)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/moneyflow_ind_dc", params)

    def moneyflow_mkt_dc(self, trade_date: str = "",
                         start_date: str = "", end_date: str = "") -> Any:
        """市场资金流向(东方财富)"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/moneyflow_mkt_dc", params)

    def moneyflow_hsgt(self, ts_code: str = "", trade_date: str = "",
                       start_date: str = "", end_date: str = "") -> Any:
        """沪深港通资金流向"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/moneyflow_hsgt", params)

    # ==================== 2.8 打板专题 ====================

    def kpl_concept(self, trade_date: str = "") -> Any:
        """开盘啦概念"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/kpl_concept", params)

    def kpl_concept_cons(self, trade_date: str = "") -> Any:
        """开盘啦概念成分"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/kpl_concept_cons", params)

    def kpl_list(self, trade_date: str = "", tag: str = "", fields: str = "") -> Any:
        """开盘啦列表"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if tag:
            params["tag"] = tag
        if fields:
            params["fields"] = fields
        return self._post("/kpl_list", params)

    def top_list(self, trade_date: str) -> Any:
        """龙虎榜-上榜"""
        return self._post("/top_list", {"trade_date": trade_date})

    def top_inst(self, trade_date: str) -> Any:
        """龙虎榜-机构"""
        return self._post("/top_inst", {"trade_date": trade_date})

    def limit_list_ths(self, trade_date: str = "", limit_type: str = "", fields: str = "") -> Any:
        """涨停列表（同花顺）"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if limit_type:
            params["limit_type"] = limit_type
        if fields:
            params["fields"] = fields
        return self._post("/limit_list_ths", params)

    def limit_list_d(self, trade_date: str = "", limit_type: str = "", fields: str = "") -> Any:
        """涨跌停明细"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if limit_type:
            params["limit_type"] = limit_type
        if fields:
            params["fields"] = fields
        return self._post("/limit_list_d", params)

    def limit_step(self, ts_code: str = "", trade_date: str = "",
                   start_date: str = "", end_date: str = "", nums: str = "") -> Any:
        """涨停阶梯"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if nums:
            params["nums"] = nums
        return self._post("/limit_step", params)

    def limit_cpt_list(self, ts_code: str = "", trade_date: str = "",
                       start_date: str = "", end_date: str = "") -> Any:
        """涨停股票池"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/limit_cpt_list", params)

    def ths_index(self) -> Any:
        """同花顺指数"""
        return self._post("/ths_index")

    def ths_member(self, ts_code: str = "") -> Any:
        """同花顺成分股"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/ths_member", params)

    def dc_index(self, ts_code: str = "", trade_date: str = "",
                start_date: str = "", end_date: str = "") -> Any:
        """东财指数"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/dc_index", params)

    def dc_member(self, ts_code: str = "", trade_date: str = "") -> Any:
        """东财成分股"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/dc_member", params)

    def stk_auction(self, ts_code: str = "", trade_date: str = "",
                    start_date: str = "", end_date: str = "") -> Any:
        """股票集合竞价"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/stk_auction", params)

    def hm_list(self) -> Any:
        """活跃股列表"""
        return self._post("/hm_list")

    def hm_detail(self, trade_date: str = "") -> Any:
        """活跃股明细"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/hm_detail", params)

    def ths_hot(self, market: str = "", trade_date: str = "", fields: str = "") -> Any:
        """同花顺热点"""
        params = {}
        if market:
            params["market"] = market
        if trade_date:
            params["trade_date"] = trade_date
        if fields:
            params["fields"] = fields
        return self._post("/ths_hot", params)

    def dc_hot(self, market: str = "", hot_type: str = "", trade_date: str = "", fields: str = "") -> Any:
        """东方财富热点"""
        params = {}
        if market:
            params["market"] = market
        if hot_type:
            params["hot_type"] = hot_type
        if trade_date:
            params["trade_date"] = trade_date
        if fields:
            params["fields"] = fields
        return self._post("/dc_hot", params)

    # ==================== 3. 指数专题 ====================

    def index_basic(self, market: str = "") -> Any:
        """指数基本信息"""
        params = {}
        if market:
            params["market"] = market
        return self._post("/index_basic", params)

    def index_daily(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """指数日线"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/index_daily", params)

    def index_weekly(self, ts_code: str = "", start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """指数周线"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/index_weekly", params)

    def index_monthly(self, ts_code: str = "", start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """指数月线"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/index_monthly", params)

    def index_weight(self, index_code: str, start_date: str = "", end_date: str = "") -> Any:
        """指数成分"""
        params = {"index_code": index_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/index_weight", params)

    def index_dailybasic(self, trade_date: str = "", fields: str = "") -> Any:
        """指数每日指标"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if fields:
            params["fields"] = fields
        return self._post("/index_dailybasic", params)

    def index_classify(self, level: str = "", src: str = "") -> Any:
        """指数分类"""
        params = {}
        if level:
            params["level"] = level
        if src:
            params["src"] = src
        return self._post("/index_classify", params)

    def index_member(self, index_code: str = "", ts_code: str = "") -> Any:
        """指数成分股"""
        params = {}
        if index_code:
            params["index_code"] = index_code
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/index_member", params)

    def index_member_all(self, l1_code: str = "", l2_code: str = "", l3_code: str = "", ts_code: str = "") -> Any:
        """指数成分股(全)"""
        params = {}
        if l1_code:
            params["l1_code"] = l1_code
        if l2_code:
            params["l2_code"] = l2_code
        if l3_code:
            params["l3_code"] = l3_code
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/index_member_all", params)

    def daily_info(self, trade_date: str = "", exchange: str = "", fields: str = "") -> Any:
        """每日信息"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if exchange:
            params["exchange"] = exchange
        if fields:
            params["fields"] = fields
        return self._post("/daily_info", params)

    def sz_daily_info(self, trade_date: str = "", ts_code: str = "") -> Any:
        """深市每日信息"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/sz_daily_info", params)

    def ths_daily(self, ts_code: str = "", start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """同花顺指数日线"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/ths_daily", params)

    def ci_daily(self, trade_date: str = "", fields: str = "") -> Any:
        """中证指数日线"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if fields:
            params["fields"] = fields
        return self._post("/ci_daily", params)

    def sw_daily(self, trade_date: str = "", fields: str = "") -> Any:
        """申万指数日线"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if fields:
            params["fields"] = fields
        return self._post("/sw_daily", params)

    def index_global(self, ts_code: str = "", start_date: str = "", end_date: str = "") -> Any:
        """全球指数"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/index_global", params)

    def idx_factor_pro(self, ts_code: str = "", trade_date: str = "",
                        start_date: str = "", end_date: str = "") -> Any:
        """指数因子(专业版)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/idx_factor_pro", params)

    # ==================== 4. 公募基金 ====================

    def fund_basic(self, market: str = "") -> Any:
        """基金基本信息"""
        params = {}
        if market:
            params["market"] = market
        return self._post("/fund_basic", params)

    def fund_company(self) -> Any:
        """基金公司"""
        return self._post("/fund_company")

    def fund_manager(self, ts_code: str) -> Any:
        """基金经理"""
        return self._post("/fund_manager", {"ts_code": ts_code})

    def fund_share(self, ts_code: str = "") -> Any:
        """基金份额"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/fund_share", params)

    def fund_nav(self, ts_code: str) -> Any:
        """基金净值"""
        return self._post("/fund_nav", {"ts_code": ts_code})

    def fund_div(self, ann_date: str = "") -> Any:
        """基金分红"""
        params = {}
        if ann_date:
            params["ann_date"] = ann_date
        return self._post("/fund_div", params)

    def fund_portfolio(self, ts_code: str) -> Any:
        """基金持仓"""
        return self._post("/fund_portfolio", {"ts_code": ts_code})

    def fund_daily(self, ts_code: str, trade_date: str = "", start_date: str = "", end_date: str = "") -> Any:
        """基金日线"""
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/fund_daily", params)

    def fund_adj(self, ts_code: str = "", start_date: str = "", end_date: str = "") -> Any:
        """基金复权"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/fund_adj", params)

    def fund_factor_pro(self, ts_code: str = "", trade_date: str = "",
                         start_date: str = "", end_date: str = "") -> Any:
        """基金因子(专业版)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/fund_factor_pro", params)

    # ==================== 5. 期货数据 ====================

    def fut_basic(self, exchange: str = "", fut_type: str = "", fields: str = "") -> Any:
        """期货基本信息"""
        params = {}
        if exchange:
            params["exchange"] = exchange
        if fut_type:
            params["fut_type"] = fut_type
        if fields:
            params["fields"] = fields
        return self._post("/fut_basic", params)

    def fut_daily(self, ts_code: str = "", trade_date: str = "",
                  start_date: str = "", end_date: str = "", exchange: str = "", fields: str = "") -> Any:
        """期货日线"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if exchange:
            params["exchange"] = exchange
        if fields:
            params["fields"] = fields
        return self._post("/fut_daily", params)

    def fut_weekly_monthly(self, ts_code: str = "", trade_date: str = "",
                           start_date: str = "", end_date: str = "", freq: str = "", exchange: str = "") -> Any:
        """期货周/月线"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if freq:
            params["freq"] = freq
        if exchange:
            params["exchange"] = exchange
        return self._post("/fut_weekly_monthly", params)

    def ft_mins(self, ts_code: str = "", start_date: str = "", end_date: str = "", freq: str = "") -> Any:
        """期货分钟线"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if freq:
            params["freq"] = freq
        return self._post("/ft_mins", params)

    def fut_wsr(self, trade_date: str = "", symbol: str = "") -> Any:
        """期货持仓"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if symbol:
            params["symbol"] = symbol
        return self._post("/fut_wsr", params)

    def fut_settle(self, trade_date: str = "", exchange: str = "") -> Any:
        """期货结算"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if exchange:
            params["exchange"] = exchange
        return self._post("/fut_settle", params)

    def fut_holding(self, trade_date: str = "", symbol: str = "", exchange: str = "") -> Any:
        """期货持仓量"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if symbol:
            params["symbol"] = symbol
        if exchange:
            params["exchange"] = exchange
        return self._post("/fut_holding", params)

    def fut_mapping(self, ts_code: str = "", trade_date: str = "",
                    start_date: str = "", end_date: str = "") -> Any:
        """期货映射"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/fut_mapping", params)

    def fut_weekly_detail(self, prd: str = "", start_week: str = "", end_week: str = "", fields: str = "") -> Any:
        """期货每周详情"""
        params = {}
        if prd:
            params["prd"] = prd
        if start_week:
            params["start_week"] = start_week
        if end_week:
            params["end_week"] = end_week
        if fields:
            params["fields"] = fields
        return self._post("/fut_weekly_detail", params)

    def ft_limit(self, trade_date: str = "", ts_code: str = "", cont: str = "") -> Any:
        """期货涨跌停"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if ts_code:
            params["ts_code"] = ts_code
        if cont:
            params["cont"] = cont
        return self._post("/ft_limit", params)

    def fut_trade_cal(self, exchange: str = "", start_date: str = "", end_date: str = "") -> Any:
        """期货交易日历"""
        params = {}
        if exchange:
            params["exchange"] = exchange
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/fut_trade_cal", params)

    # ==================== 6. 现货数据 ====================

    def sge_basic(self, ts_code: str = "") -> Any:
        """现货基本信息"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        return self._post("/sge_basic", params)

    def sge_daily(self, trade_date: str = "", prd: str = "",
                  start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """现货每日行情"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if prd:
            params["prd"] = prd
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/sge_daily", params)

    # ==================== 7. 期权数据 ====================

    def opt_basic(self, exchange: str = "", fields: str = "") -> Any:
        """期权基本信息"""
        params = {}
        if exchange:
            params["exchange"] = exchange
        if fields:
            params["fields"] = fields
        return self._post("/opt_basic", params)

    def opt_daily(self, ts_code: str = "", trade_date: str = "",
                  start_date: str = "", end_date: str = "", exchange: str = "") -> Any:
        """期权每日行情"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if exchange:
            params["exchange"] = exchange
        return self._post("/opt_daily", params)

    # ==================== 8. 可转债/债券数据 ====================

    def cb_basic(self, fields: str = "") -> Any:
        """可转债基本信息"""
        params = {}
        if fields:
            params["fields"] = fields
        return self._post("/cb_basic", params)

    def cb_issue(self, ann_date: str = "", fields: str = "") -> Any:
        """可转债发行"""
        params = {}
        if ann_date:
            params["ann_date"] = ann_date
        if fields:
            params["fields"] = fields
        return self._post("/cb_issue", params)

    def cb_call(self, fields: str = "") -> Any:
        """可转债回售"""
        params = {}
        if fields:
            params["fields"] = fields
        return self._post("/cb_call", params)

    def cb_rate(self, ts_code: str = "", fields: str = "") -> Any:
        """可转债转股溢价率"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if fields:
            params["fields"] = fields
        return self._post("/cb_rate", params)

    def cb_daily(self, trade_date: str = "", ts_code: str = "",
                 start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """可转债每日行情"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/cb_daily", params)

    def cb_price_chg(self, ts_code: str = "", fields: str = "") -> Any:
        """可转债价格变化"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if fields:
            params["fields"] = fields
        return self._post("/cb_price_chg", params)

    def cb_share(self, ts_code: str = "", fields: str = "") -> Any:
        """可转债转股"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if fields:
            params["fields"] = fields
        return self._post("/cb_share", params)

    def cb_factor_pro(self, ts_code: str = "", trade_date: str = "",
                      start_date: str = "", end_date: str = "") -> Any:
        """可转债因子(专业版)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/cb_factor_pro", params)

    def repo_daily(self, trade_date: str = "") -> Any:
        """回购每日行情"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/repo_daily", params)

    def bc_otcqt(self, ts_code: str = "", start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """银行间报价"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/bc_otcqt", params)

    def bc_bestotcqt(self, ts_code: str = "", start_date: str = "", end_date: str = "", fields: str = "") -> Any:
        """银行间最优报价"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if fields:
            params["fields"] = fields
        return self._post("/bc_bestotcqt", params)

    def bond_blk(self, start_date: str = "", end_date: str = "") -> Any:
        """债券大宗交易"""
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/bond_blk", params)

    def bond_blk_detail(self, start_date: str = "", end_date: str = "") -> Any:
        """债券大宗交易明细"""
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/bond_blk_detail", params)

    def yc_cb(self, trade_date: str = "", curve_type: str = "") -> Any:
        """可转债收益率"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if curve_type:
            params["curve_type"] = curve_type
        return self._post("/yc_cb", params)

    # ==================== 9. 宏观经济 ====================

    def eco_cal(self, date: str = "", country: str = "", event: str = "", fields: str = "") -> Any:
        """经济日历"""
        params = {}
        if date:
            params["date"] = date
        if country:
            params["country"] = country
        if event:
            params["event"] = event
        if fields:
            params["fields"] = fields
        return self._post("/eco_cal", params)

    def shibor(self, start_date: str = "", end_date: str = "") -> Any:
        """Shibor利率"""
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/shibor", params)

    def shibor_quote(self, start_date: str = "", end_date: str = "") -> Any:
        """Shibor报价"""
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/shibor_quote", params)

    def cn_gdp(self, quarter: str = "", start_date: str = "", end_date: str = "") -> Any:
        """中国GDP"""
        params = {}
        if quarter:
            params["quarter"] = quarter
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/cn_gdp", params)

    def cn_cpi(self, month: str = "", start_date: str = "", end_date: str = "") -> Any:
        """中国CPI"""
        params = {}
        if month:
            params["month"] = month
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/cn_cpi", params)

    def cn_ppi(self, month: str = "", start_date: str = "", end_date: str = "") -> Any:
        """中国PPI"""
        params = {}
        if month:
            params["month"] = month
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/cn_ppi", params)

    def sf_month(self, start_month: str = "", end_month: str = "") -> Any:
        """上海黄金现货月报"""
        params = {}
        if start_month:
            params["start_month"] = start_month
        if end_month:
            params["end_month"] = end_month
        return self._post("/sf_month", params)

    # ==================== 10. 外汇数据 ====================

    def fx_obasic(self, exchange: str = "", classify: str = "", fields: str = "") -> Any:
        """外汇基本信息"""
        params = {}
        if exchange:
            params["exchange"] = exchange
        if classify:
            params["classify"] = classify
        if fields:
            params["fields"] = fields
        return self._post("/fx_obasic", params)

    def fx_daily(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """外汇每日行情"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/fx_daily", params)

    # ==================== 11. 港股数据 ====================

    def hk_basic(self, list_status: str = "", trade_date: str = "") -> Any:
        """港股基本信息"""
        params = {}
        if list_status:
            params["list_status"] = list_status
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/hk_basic", params)

    def hk_tradecal(self, start_date: str = "", end_date: str = "") -> Any:
        """港股交易日历"""
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/hk_tradecal", params)

    def hk_daily(self, ts_code: str = "", trade_date: str = "",
                 start_date: str = "", end_date: str = "") -> Any:
        """港股每日行情"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/hk_daily", params)

    def hk_daily_adj(self, ts_code: str = "", trade_date: str = "",
                     start_date: str = "", end_date: str = "") -> Any:
        """港股每日行情(复权)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/hk_daily_adj", params)

    def hk_mins(self, ts_code: str, start_date: str = "", end_date: str = "", freq: str = "") -> Any:
        """港股分钟线"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if freq:
            params["freq"] = freq
        return self._post("/hk_mins", params)

    def hk_income(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """港股利润表"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/hk_income", params)

    def hk_balancesheet(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """港股资产负债表"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/hk_balancesheet", params)

    def hk_cashflow(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """港股现金流量表"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/hk_cashflow", params)

    # ==================== 12. 美股数据 ====================

    def us_basic(self) -> Any:
        """美股基本信息"""
        return self._post("/us_basic")

    def us_tradecal(self, start_date: str = "", end_date: str = "") -> Any:
        """美股交易日历"""
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/us_tradecal", params)

    def us_daily(self, ts_code: str = "", trade_date: str = "",
                 start_date: str = "", end_date: str = "") -> Any:
        """美股每日行情"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/us_daily", params)

    def us_daily_adj(self, ts_code: str = "", trade_date: str = "",
                     start_date: str = "", end_date: str = "", exchange: str = "") -> Any:
        """美股每日行情(复权)"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if exchange:
            params["exchange"] = exchange
        return self._post("/us_daily_adj", params)

    def us_income(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """美股利润表"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/us_income", params)

    def us_balancesheet(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """美股资产负债表"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/us_balancesheet", params)

    def us_cashflow(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """美股现金流量表"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/us_cashflow", params)

    # ==================== 13. ETF/ETF数据 ====================

    def etf_basic(self, market: str = "") -> Any:
        """ETF基本信息"""
        params = {}
        if market:
            params["market"] = market
        return self._post("/etf_basic", params)

    def etf_index(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """ETF指数跟踪"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/etf_index", params)

    def etf_share_size(self, ts_code: str) -> Any:
        """ETF份额变化"""
        params = {"ts_code": ts_code}
        return self._post("/etf_share_size", params)

    # ==================== 14. 特色指数 ====================

    def dc_daily(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """东财指数日线"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/dc_daily", params)

    def gz_index(self, ts_code: str = "", trade_date: str = "",
                 start_date: str = "", end_date: str = "") -> Any:
        """国证指数"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/gz_index", params)

    def wz_index(self, ts_code: str = "", trade_date: str = "",
                 start_date: str = "", end_date: str = "") -> Any:
        """万德指数"""
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/wz_index", params)

    def tdx_index(self, ts_code: str) -> Any:
        """通达信指数"""
        params = {"ts_code": ts_code}
        return self._post("/tdx_index", params)

    def tdx_daily(self, ts_code: str, start_date: str = "", end_date: str = "") -> Any:
        """通达信指数日线"""
        params = {"ts_code": ts_code}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/tdx_daily", params)

    # ==================== 15. 资讯数据 ====================

    def news(self, src: str = "", start_date: str = "", end_date: str = "") -> Any:
        """资讯"""
        params = {}
        if src:
            params["src"] = src
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/news", params)

    def major_news(self, trade_date: str = "", start_date: str = "", end_date: str = "") -> Any:
        """重要资讯"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/major_news", params)

    # ==================== 16. 基金销售 ====================

    def fund_sales_ratio(self, ts_code: str) -> Any:
        """基金销售比例"""
        params = {"ts_code": ts_code}
        return self._post("/fund_sales_ratio", params)

    def fund_sales_vol(self, ts_code: str) -> Any:
        """基金销售量"""
        params = {"ts_code": ts_code}
        return self._post("/fund_sales_vol", params)

    # ==================== 17. 其他高价值数据 ====================

    def stk_account(self, start_date: str = "", end_date: str = "") -> Any:
        """新增开户数"""
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._post("/stk_account", params)

    def stk_ah_comparison(self, trade_date: str = "") -> Any:
        """AH股比价"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/stk_ah_comparison", params)

    def ci_index_member(self, index_code: str) -> Any:
        """中证指数成分股权"""
        params = {"index_code": index_code}
        return self._post("/ci_index_member", params)

    def dc_moneyflow_stock(self, trade_date: str = "") -> Any:
        """东财个股资金流向"""
        params = {}
        if trade_date:
            params["trade_date"] = trade_date
        return self._post("/dc_moneyflow_stock", params)


# ==================== 便捷函数 ====================

def get_daily(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取日线数据"""
    st = StockToday()
    data = st.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if isinstance(data, list):
        return pd.DataFrame(data)
    return pd.DataFrame()


def get_index_daily(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取指数日线"""
    st = StockToday()
    data = st.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if isinstance(data, list):
        return pd.DataFrame(data)
    return pd.DataFrame()


def get_realtime_quote(ts_code: str) -> Dict:
    """获取实时行情（使用 pro_bar）"""
    st = StockToday()
    from datetime import datetime, timedelta
    today = datetime.now().strftime("%Y%m%d")
    return st.pro_bar(ts_code=ts_code, start_date=today, end_date=today)


if __name__ == "__main__":
    st = StockToday()

    # 测试示例
    print(st.daily(ts_code="600519.SH", start_date="20250101", end_date="20250110"))
    # print(st.index_daily(ts_code="000001.SH", start_date="20250101", end_date="20250110"))
    # print(st.stk_auction(trade_date="20250903"))
    # print(st.margin(trade_date="20250110"))
    # print(st.top_list(trade_date="20250110"))
    # print(st.fund_basic())
