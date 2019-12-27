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
        config = {"AShareCalendar": "jqdata"}
        return config



