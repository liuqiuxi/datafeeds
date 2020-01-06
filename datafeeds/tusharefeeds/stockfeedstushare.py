# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 16:06
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : stockfeedstushare.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of stock market

import datetime
import time
import pandas as pd
from datafeeds.tusharefeeds import BaseTuShare
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class AShareCalendarTuShare(BaseTuShare):

    def __init__(self):
        super(AShareCalendarTuShare, self).__init__()

    def get_calendar(self, begin_datetime, end_datetime):
        connect = self.connect_pro()
        begin_datetime = begin_datetime.strftime("%Y%m%d")
        end_datetime = end_datetime.strftime("%Y%m%d")
        data = connect.query(api_name="trade_cal", start_date=begin_datetime, end_date=end_datetime)
        data = data.loc[data.loc[:, "is_open"] == 1, :].copy(deep=True)
        data.drop_duplicates(subset=["cal_date"], keep="first", inplace=True)
        data.rename(columns={"cal_date": "dateTime"}, inplace=True)
        data.loc[:, "dateTime"] = data.loc[:, "dateTime"].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data = pd.DataFrame(data={"dateTime": data.loc[:, "dateTime"]})
        data.sort_values(by="dateTime", axis=0, ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        return data


class AShareQuotationTuShare(BaseTuShare):
    LOGGER_NAME = "AShareQuotationTuShare"

    def __init__(self):
        super(AShareQuotationTuShare, self).__init__()
        self.__need_adjust_columns = ["preClose", "open", "high", "low", "close", "volume", "avgPrice"]
        self.__adjust_name_dict = {"F": "qfq", "B": "hfq"}


    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted="F"):
        log = logger.get_logger(name=self.LOGGER_NAME)
        connect = self.connect_ts()
        api = self.connect_pro()
        securityIds = self.wind_to_default(securityIds=securityIds)
        frequency = self.get_frequency_cycle(frequency=frequency)
        begin_datetime = begin_datetime.strftime("%Y%m%d")
        end_datetime = end_datetime.strftime("%Y%m%d")
        adjusted = self.__adjust_name_dict.get(adjusted, None)
        rename_dict = BarFeedConfig.get_tushare_items().get(self.LOGGER_NAME)
        data = pd.DataFrame()
        for securityId in securityIds:
            data0 = connect.pro_bar(ts_code=securityId, api=api, start_date=begin_datetime, end_date=end_datetime,
                                    freq=frequency, asset="E", adj=adjusted, adjfactor=False)
            securityId = self.default_to_wind(securityIds=[securityId])
            data0.loc[:, "ts_code"] = securityId
            # 对于分钟级数据， tushare有限制
            if "trade_time" in data0.columns:
                data0.loc[:, "dateTime"] = data0.loc[:, "trade_time"].apply(
                                                    lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
                log.warning("The tushare only support get min data twice in one minute")
                wait_time = BarFeedConfig.get_tushare().get("LimitNumbers").get("min_quotation_wait")
                time.sleep(wait_time)
            else:
                data0.loc[:, "dateTime"] = data0.loc[:, "trade_date"].apply(
                                                    lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
            data = pd.concat(objs=[data, data0], axis=0, join="outer")
        data.rename(columns=rename_dict, inplace=True)
        # change some parameters
        if "Chg" in data.columns:
            data.loc[:, "Chg"] = data.loc[:, "Chg"] / 100
        # choose items to data
        default_items = data.columns
        real_items = []
        for item in items:
            if item in ["securityId", "dateTime"]:
                log.info("There is no need add item: %s to parameters items" % item)
            elif item in default_items:
                real_items.append(item)
            else:
                log.warning("item %s not in default items, so we remove this item to data" % item)
        data = data.loc[:, ["dateTime", "securityId"] + real_items].copy(deep=True)
        data.reset_index(inplace=True, drop=True)
        data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
        data.reset_index(inplace=True, drop=True)
        return data











