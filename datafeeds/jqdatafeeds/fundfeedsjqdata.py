# -*- coding:utf-8 -*-
# @Time    : 2020/1/122:48
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : fundfeedsjqdata.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of option market

import pandas as pd
from datafeeds.jqdatafeeds import BaseJqData
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class AFundQuotationJqData(BaseJqData):
    LOGGER_NAME = "AFundQuotationJqData"

    def __init__(self):
        super(AFundQuotationJqData, self).__init__()
        self.__adjust_name_dict = {"F": "pre", "B": "post"}

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
        log = logger.get_logger(name=self.LOGGER_NAME)
        securityIds_OTC = []
        securityIds_EXC = []
        # 判断场内场外基金
        for securityId in securityIds:
            code_suffix = securityId[securityId.find(".") + 1:]
            if code_suffix == "OF":
                securityIds_OTC.append(securityId)
            elif code_suffix == "SH" or code_suffix == "SZ":
                securityIds_EXC.append(securityId)
            else:
                log.warning("the securityId: %s did't support in fund quotation, we remove it" % securityId)
        # 得到场内基金数据
        pass

    def get_exchange_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted):
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

    def get_otc_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted):
        connect = self.connect()
        # jqdata OTC code did't need suffix
        for i in range(len(securityIds)):
            securityId = securityIds[i]
            securityId = securityId.replace(".OF", "")
            securityIds[i] = securityId
            pass















