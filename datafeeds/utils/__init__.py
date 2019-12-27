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
        config = {"token": "592204bae01431106102416727abc9b4a40e56841722d3ab66c2a466",
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
        config = {"Server": "26.4.0.60",
                  "Port": "1522",
                  "DateBase": "wideaprd",
                  "UserId": "ideawindprd_ro",
                  "PassWord": "paic12345",
                  "LimitNumbers": 1000
                  }
        return config

    @staticmethod
    def get_jqdata():
        config = {"username": "18244236905",
                  "password": "123456abc"
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
        config = {"AShareCalendar": "tushare"}
        return config



