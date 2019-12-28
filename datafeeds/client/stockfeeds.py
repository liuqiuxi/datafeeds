# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 16:45
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : stockfeeds.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is all data source stock feeds

import datetime
from datafeeds.utils import BarFeedConfig
from datafeeds import logger
from datafeeds.winddatabasefeeds.stockfeedswinddatabase import AShareCalendarWindDataBase
from datafeeds.tusharefeeds.stockfeedstushare import AShareCalendarTuShare
from datafeeds.jqdatafeeds.stockfeedsjqdata import AShareCalendarJqData
from datafeeds.windclientfeeds.stockfeedswindclient import AShareCalendarWindClient


class AShareCalendar:

    @staticmethod
    def get_calendar(begin_datetime, end_datetime, dataSource=None):
        if not isinstance(begin_datetime, datetime.datetime):
            raise BaseException("[AShareCalendar] begin_datetime must be datetime.datetime")
        if not isinstance(end_datetime, datetime.datetime):
            raise BaseException("[AShareCalendar] end_datetime must be datetime.datetime")
        if begin_datetime > end_datetime:
            raise BaseException("[AShareCalendar] begin_datetime must less than end_datetime")
        if dataSource is None:
            log = logger.get_logger(name="AShareCalendar")
            dataSource = BarFeedConfig.get_client_config().get("AShareCalendar")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            data = AShareCalendarWindDataBase().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        elif dataSource == "tushare":
            data = AShareCalendarTuShare().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        elif dataSource == "jqdata":
            data = AShareCalendarJqData().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        elif dataSource == "windclient":
            data = AShareCalendarWindClient().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        else:
            raise BaseException("[%s] dataSource: %s can't supply now" % dataSource)
        data.loc[:, "dateSource"] = dataSource
        data.drop_duplicates(subset=["dateTime"], inplace=True)
        data.sort_values(by="dateTime", axis=0, ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        return data






