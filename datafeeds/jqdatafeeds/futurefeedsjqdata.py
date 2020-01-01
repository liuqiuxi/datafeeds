# -*- coding:utf-8 -*-
# @Time    : 2020/1/117:19
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : futurefeedsjqdata.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of future market

import pandas as pd
from datafeeds.jqdatafeeds import BaseJqData
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class AFutureQuotationJqData(BaseJqData):
    LOGGER_NAME = "AFutureQuotationJqData"

    def __init__(self):
        super(AFutureQuotationJqData, self).__init__()

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
        log = logger.get_logger(name=self.LOGGER_NAME)
        if adjusted is not None:
            log.info("Future data has no adjust price, so we change adjusted %s to None" % adjusted)
            adjusted = None
        connect = self.connect()
        securityIds = self.wind_to_default(securityIds=securityIds)
        frequency = self.get_frequency_cycle(frequency=frequency)
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

