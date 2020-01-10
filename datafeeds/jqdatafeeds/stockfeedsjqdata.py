# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 15:48
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : stockfeedsjqdata.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of stock market

import datetime
import pandas as pd
from datafeeds.jqdatafeeds import BaseJqData
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class AShareCalendarJqData(BaseJqData):

    def __init__(self):
        super(AShareCalendarJqData, self).__init__()

    def get_calendar(self, begin_datetime, end_datetime):
        connect = self.connect()
        data = connect.get_trade_days(start_date=begin_datetime, end_date=end_datetime, count=None)
        data = pd.DataFrame(data={"dateTime": data})
        datetime_list = data.loc[:, "dateTime"].apply(
            lambda x: datetime.datetime.combine(date=x, time=datetime.time.min))
        data.loc[:, "dateTime"] = datetime_list
        data.drop_duplicates(subset=["dateTime"], keep="first", inplace=True)
        data.sort_values(by="dateTime", axis=0, ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        connect.logout()
        return data


class AShareQuotationJqData(BaseJqData):
    LOGGER_NAME = "AShareQuotationJqData"

    def __init__(self):
        super(AShareQuotationJqData, self).__init__()
        self.__adjust_name_dict = {"F": "pre", "B": "post"}

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
        connect = self.connect()
        securityIds = self.wind_to_default(securityIds=securityIds)
        frequency = self.get_frequency_cycle(frequency=frequency)
        adjusted = self.__adjust_name_dict.get(adjusted, None)
        rename_dict = BarFeedConfig.get_jq_data_items().get(self.LOGGER_NAME)
        data = pd.DataFrame()
        for securityId in securityIds:
            data0 = connect.get_price(security=securityId, start_date=begin_datetime, end_date=end_datetime,
                                      frequency=frequency, skip_paused=False, fq=adjusted)
            data0.loc[:, "dateTime"] = data0.index
            securityId = self.default_to_wind(securityIds=[securityId])
            data0.loc[:, "securityId"] = securityId
            data = pd.concat(objs=[data, data0], axis=0, join="outer")
        data.rename(columns=rename_dict, inplace=True)
        # choose items to data
        log = logger.get_logger(name=self.LOGGER_NAME)
        default_items = list(rename_dict.values())
        real_items = []
        for item in items:
            if item in ["securityId", "dateTime"]:
                log.info("There is no need add item: %s to parameters items" % item)
            elif item in default_items:
                real_items.append(item)
            else:
                log.warning("item %s not in default items, so we remove this item to data" % item)
        data = data.loc[:, ["dateTime", "securityId"] + real_items].copy(deep=True)
        connect.logout()
        return data










