# -*- coding:utf-8 -*-
# @Time    : 2019-12-26 16:38
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : __init__.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This file used for config files

import logging


class BarFeedConfig:

    @staticmethod
    def get_tushare():
        config = {"token": "",
                  "futureExchange": ["CFFEX", "DCE", "CZCE", "SHFE", "INE"],
                  "indexExchange": ["MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH"],
                  "fundExchange": ["E", "O"],
                  "optionExchange": ["CZCE", "SHFE", "DCE", "SSE"],
                  "LimitNumbers": {"min_quotation_wait": 30
                                   }
                  }
        return config

    @staticmethod
    def get_wind():
        config = {"Server": "",
                  "Port": "",
                  "DateBase": "",
                  "UserId": "",
                  "PassWord": "",
                  "LimitNumbers": 1000
                  }
        return config

    @staticmethod
    def get_jqdata():
        config = {"username": "",
                  "password": ""
                  }
        return config

    @staticmethod
    def get_logger_config():
        config = {"log_format": "%(asctime)s %(name)s [%(levelname)s] %(message)s",
                  "level": logging.INFO,
                  "file_log": None, # you can write file name here
                  "console_log": True
                  }
        return config

    @staticmethod
    def get_client_config():
        config = {"AShareCalendar": "jqdata",
                  "AShareQuotation": "wind",
                  "AIndexQuotation": "wind",
                  "AFutureQuotation": "wind",
                  "AOptionQuotation": "wind",
                  "AFundQuotation": "wind",
                  "AIndexWeights": "wind",
                  "AShareIndustry": "windclient",
                  "AShareIPO": "wind",
                  "AShareDayVars": "wind"
                  }
        return config

    @staticmethod
    def get_wind_database_items():
        config = {"AShareQuotationWindDataBase": {"s_info_windcode": "securityId", "trade_dt": "dateTime",
                                                  "s_dq_preclose": "preClose", "s_dq_open": "open", "s_dq_high": "high",
                                                  "s_dq_low": "low", "s_dq_close": "close",
                                                  "s_dq_change": "cash_change", "s_dq_pctchange": "Chg",
                                                  "s_dq_volume": "volume", "s_dq_amount": "amount",
                                                  "s_dq_avgprice": "avgPrice", "s_dq_adjfactor": "adjfactor",
                                                  "s_dq_tradestatus": "tradeStatus"},

                  "AIndexQuotationWindDataBase": {"s_info_windcode": "securityId", "trade_dt": "dateTime",
                                                  "s_dq_preclose": "preClose", "s_dq_open": "open", "s_dq_high": "high",
                                                  "s_dq_low": "low", "s_dq_close": "close",
                                                  "s_dq_change": "cash_change", "s_dq_pctchange": "Chg",
                                                  "s_dq_volume": "volume", "s_dq_amount": "amount"},

                  "AFutureQuotationWindDataBase": {"s_info_windcode": "securityId", "trade_dt": "dateTime",
                                                   "s_dq_presettle": "preSettle", "s_dq_open": "open",
                                                   "s_dq_high": "high", "s_dq_low": "low", "s_dq_close": "close",
                                                   "s_dq_settle": "settle", "s_dq_volume": "volume",
                                                   "s_dq_amount": "amount", "s_dq_oi": "openInterest",
                                                   "s_dq_change": "cash_change_close_to_settle",
                                                   "s_dq_oichange": "openInterestChange"},

                  "AOptionQuotationWindDataBase": {"s_info_windcode": "securityId", "trade_dt": "dateTime",
                                                   "s_dq_open": "open", "s_dq_high": "high", "s_dq_low": "low",
                                                   "s_dq_close": "close", "s_dq_settle": "settle",
                                                   "s_dq_volume": "volume", "s_dq_amount": "amount",
                                                   "s_dq_oi": "openInterest", "s_dq_oichange": "openInterestChange",
                                                   "s_dq_presettle": "preSettle",
                                                   "s_dq_change1": "cash_change_close_to_settle",
                                                   "s_dq_change2": "cash_change_settle_to_settle"},

                  "AFundQuotationWindDataBase": {"s_info_windcode": "securityId", "trade_dt": "dateTime",
                                                 "s_dq_preclose": "preClose", "s_dq_open": "open", "s_dq_high": "high",
                                                 "s_dq_low": "low", "s_dq_close": "close",
                                                 "s_dq_change": "cash_change", "s_dq_pctchange": "Chg",
                                                 "s_dq_volume": "volume", "s_dq_amount": "amount",
                                                 "s_dq_adjfactor": "adjfactor", "f_info_windcode": "securityId",
                                                 "price_date": "dateTime", "f_nav_unit": "close",
                                                 "f_nav_adjfactor": "adjfactor"},

                  "AShareIPOWindDataBase": {"s_info_windcode": "securityId", "s_ipo_price": "ipoPrice",
                                            "s_ipo_ pre_ dilutedpe": "predilutedPE", "s_ipo_dilutedpe": "dilutedPE",
                                            "s_ipo_amount": "amount", "s_ipo_collection": "collection",
                                            "s_ipo_subdate": "subDate", "s_ipo_listdate": "listDate"},

                  "AShareDayVarsWindDataBase": {"s_info_windcode": "securityId", "trade_dt": "dateTime",
                                                "s_val_mv": "totalValue", "s_dq_mv": "marketValue", "s_val_pe": "PE",
                                                "s_val_pb_new": "PB", "s_val_pe_ttm": "PE_TTM",
                                                "s_val_pcf_ocf": "PCF_OCF", "s_val_pcf_ocfttm": "PCF_OCF_TTM",
                                                "s_val_pcf_ncf": "PCF_NCF", "s_val_pcf_ncfttm": "PCF_NCF_TTM",
                                                "s_val_ps": "PS", "s_val_ps_ttm": "PS_TTM", "s_dq_turn": "turnover",
                                                "s_dq_freeturnover": "turnover_free", "s_dq_close_today": "close",
                                                "up_down_limit_status": "upOrdown"}

                  }
        return config

    @staticmethod
    def get_jq_data_items():
        config = {"AShareQuotationJqData": {"open": "open", "high": "high", "low": "low", "close": "close",
                                            "volume": "volume", "money": "amount"},
                  "AFutureQuotationJqData": {"open": "open", "high": "high", "low": "low", "close": "close",
                                             "volume": "volume", "money": "amount"},
                  "AIndexQuotationJqData": {"open": "open", "high": "high", "low": "low", "close": "close",
                                            "volume": "volume", "money": "amount"},
                  "AOptionQuotationJqData": {"open": "open", "high": "high", "low": "low", "close": "close",
                                             "volume": "volume", "money": "amount"},
                  "AFundQuotationJqData": {"open": "open", "high": "high", "low": "low", "close": "close",
                                           "volume": "volume", "money": "amount", "code": "securityId",
                                           "day": "dateTime", "net_value": "close", "acc_factor": "adjfactor"}

                  }
        return config

    @staticmethod
    def get_tushare_items():
        config = {"AShareQuotationTuShare": {"ts_code": "securityId", "open": "open", "high": "high", "low": "low",
                                             "close": "close", "pre_close": "preClose", "change": "cash_change",
                                             "pct_chg": "Chg", "vol": "volume", "amount": "amount"},

                  "AFutureQuotationTuShare": {"ts_code": "securityId", "pre_close": "preClose",
                                              "pre_settle": "preSettle", "open": "open", "high": "high", "low": "low",
                                              "close": "close", "settle": "settle",
                                              "change1": "cash_change_close_to_settle",
                                              "change2": "cash_change_settle_to_settle", "vol": "volume",
                                              "amount": "amount", "oi": "openInterest", "oi_chg": "openInterestChange"}

                  }
        return config
