# -*- coding:utf-8 -*-
# @Time    : 2020/1/1019:55
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : futurefeedstushare.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of stock market

import datetime
import time
import pandas as pd
from datafeeds.tusharefeeds import BaseTuShare
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class AFutureQuotationTuShare(BaseTuShare):
    LOGGER_NAME = "AFutureQuotationTuShare"

    def __init__(self):
        super(AFutureQuotationTuShare, self).__init__()

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
        log = logger.get_logger(name=self.LOGGER_NAME)
        if adjusted is not None:
            log.info("Future data has no adjust price, so we change adjusted %s to None" % adjusted)
            adjusted = None
        connect = self.connect_ts()
        api = self.connect_pro()
        securityIds = self.wind_to_default(securityIds=securityIds)
        frequency = self.get_frequency_cycle(frequency=frequency)
        begin_datetime = begin_datetime.strftime("%Y%m%d")
        end_datetime = end_datetime.strftime("%Y%m%d")
        rename_dict = BarFeedConfig.get_tushare_items().get(self.LOGGER_NAME)
        data = pd.DataFrame()
        for securityId in securityIds:
            data0 = connect.pro_bar(ts_code=securityId, api=api, start_date=begin_datetime, end_date=end_datetime,
                                    freq=frequency, asset="FT", adj=adjusted, adjfactor=False)
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






