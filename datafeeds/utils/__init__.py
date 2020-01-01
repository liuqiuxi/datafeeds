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
                  "LimitNumbers": {"daily_quotation": 4000
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
                  "AShareQuotation": "jqdata",
                  "AIndexQuotation": "jqdata",
                  "AFutureQuotation": "jqdata",
                  "AOptionQuotation": "jqdata",
                  "AFundQuotation": "jqdata"
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
                                                 "f_nav_adjfactor": "adjfactor"}
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
                                           "volume": "volume", "money": "amount"}

                  }
        return config






