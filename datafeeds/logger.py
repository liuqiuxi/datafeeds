# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 10:06
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : logger.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is datafeeds logging files, all longging config is here


import logging
import threading
import copy
from datafeeds.utils import BarFeedConfig

ROOT_LOGGER_INITIALIZED = False


def init_handler(handler):
    log_format = copy.deepcopy(BarFeedConfig.get_logger_config().get("log_format"))
    handler.setFormatter(Formatter(log_format))


def init_logger(logger):
    level = copy.deepcopy(BarFeedConfig.get_logger_config().get("level"))
    file_log = copy.deepcopy(BarFeedConfig.get_logger_config().get("file_log"))
    console_log = copy.deepcopy(BarFeedConfig.get_logger_config().get("console_log"))
    logger.setLevel(level)
    if file_log is not None:
        file_handler = logging.FileHandler(filename=file_log)
        init_handler(handler=file_handler)
        logger.addHandler(file_handler)
    if console_log:
        console_handler = logging.StreamHandler()
        init_handler(handler=console_handler)
        logger.addHandler(console_handler)


def initialize():
    global ROOT_LOGGER_INITIALIZED
    with threading.Lock():
        if not ROOT_LOGGER_INITIALIZED:
            init_logger(logging.getLogger())
            ROOT_LOGGER_INITIALIZED = True


def get_logger(name=None):
    initialize()
    return logging.getLogger(name=name)


# This formatter provides a way to hook in formatTime.
class Formatter(logging.Formatter):
    DATETIME_HOOK = None

    def formatTime(self, record, date_fmt=None):
        new_date_time = None

        if Formatter.DATETIME_HOOK is not None:
            new_date_time = Formatter.DATETIME_HOOK()

        if new_date_time is None:
            ret = super(Formatter, self).formatTime(record, date_fmt)
        else:
            ret = str(new_date_time)
        return ret
