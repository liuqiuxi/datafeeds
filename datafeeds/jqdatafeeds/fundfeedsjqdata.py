# -*- coding:utf-8 -*-
# @Time    : 2020/1/122:48
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : fundfeedsjqdata.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of option market

import datetime
import copy
import pandas as pd
from datafeeds.jqdatafeeds import BaseJqData
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class AFundQuotationJqData(BaseJqData):
    LOGGER_NAME = "AFundQuotationJqData"

    def __init__(self):
        super(AFundQuotationJqData, self).__init__()
        self.__adjust_name_dict = {"F": "pre", "B": "post"}
        self.__need_adjust_columns = ["close"]
        self.__logger = logger.get_logger(name=self.LOGGER_NAME)

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
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
                self.__logger.warning("the securityId: %s did't support in fund quotation, we remove it" % securityId)
        # 得到场内基金数据
        if len(securityIds_EXC) > 0:
            data0 = self.__get_exchange_quotation(securityIds=securityIds_EXC, items=items, frequency=frequency,
                                                  begin_datetime=begin_datetime, end_datetime=end_datetime,
                                                  adjusted=adjusted)
        else:
            data0 = pd.DataFrame()
        # 得到场外基金数据
        if len(securityIds_OTC) > 0:
            data1 = self.__get_otc_quotation(securityIds=securityIds_OTC, items=items, frequency=frequency,
                                             begin_datetime=begin_datetime, end_datetime=end_datetime,
                                             adjusted=adjusted)
        else:
            data1 = pd.DataFrame()
        # merge OTC and EXC
        if not data0.empty and not data1.empty:
            columns = list(set(data0.columns).union(set(data1.columns)))
            for column in columns:
                if column not in data0.columns:
                    data0.loc[:, column] = None
                if column not in data1.columns:
                    data1.loc[:, column] = None
            data0 = data0.loc[:, columns].copy(deep=True)
            data1 = data1.loc[:, columns].copy(deep=True)
            data = pd.concat(objs=[data0, data1], axis=0, join="outer")
        else:
            if data0.empty:
                data = data1.copy(deep=True)
            elif data1.empty:
                data = data0.copy(deep=True)
            else:
                raise BaseException("[AFundQuotationJqData] something may wrong")
        data.reset_index(inplace=True, drop=True)
        data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
        data.reset_index(inplace=True, drop=True)
        non_find_securityIds = list(set(securityIds) - set(data.loc[:, "securityId"]))
        if len(non_find_securityIds) > 0:
            self.__logger.warning("we can't get securityIds: %s data, please check it" % non_find_securityIds)
        return data

    def __get_exchange_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted):
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
        default_items = list(rename_dict.values())
        real_items = []
        for item in items:
            if item in ["securityId", "dateTime"]:
                self.__logger.info("There is no need add item: %s to parameters items" % item)
            elif item in default_items:
                real_items.append(item)
            else:
                self.__logger.warning("item %s not in default items, so we remove this item to data" % item)
        data = data.loc[:, ["dateTime", "securityId"] + real_items].copy(deep=True)
        connect.logout()
        return data

    def __get_otc_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted):
        connect = self.connect()
        finance = connect.finance
        if frequency != 86400:
            self.__logger.warning("otc quotation only support daily data, the frequency: %d not support now")
            return pd.DataFrame()
        begin_datetime = begin_datetime.date()
        end_datetime = end_datetime.date()
        rename_dict = BarFeedConfig.get_jq_data_items().get(self.LOGGER_NAME)
        # jqdata OTC code did't need suffix
        for i in range(len(securityIds)):
            securityId = securityIds[i]
            securityId = securityId.replace(".OF", "")
            securityIds[i] = securityId
        data = pd.DataFrame()
        for securityId in securityIds:
            q = connect.query(finance.FUND_NET_VALUE).filter(finance.FUND_NET_VALUE.code == securityId,
                                                             finance.FUND_NET_VALUE.day >= begin_datetime,
                                                             finance.FUND_NET_VALUE.day <= end_datetime)
            data0 = finance.run_query(q)
            data = pd.concat(objs=[data, data0], axis=0, join="outer")
        data.rename(columns=rename_dict, inplace=True)
        data.loc[:, "securityId"] = data.loc[:, "securityId"].apply(lambda x: x + ".OF")
        data.loc[:, "dateTime"] = data.loc[:, "dateTime"].apply(
            lambda x: datetime.datetime.combine(x, datetime.datetime.min.time()))
        # use adjfactor get adj price
        if adjusted in ["F", "B"]:
            data = data.groupby(by="securityId").apply(lambda x: self.__get_adj_price(DataFrame=x, adjusted=adjusted))
        data.reset_index(inplace=True, drop=True)
        data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
        data.reset_index(inplace=True, drop=True)
        default_items = list(data.columns)
        real_items = []
        for item in items:
            if item in ["securityId", "dateTime"]:
                self.__logger.info("There is no need add item: %s to parameters items" % item)
            elif item in default_items:
                real_items.append(item)
            else:
                self.__logger.warning("item %s not in default items, so we remove this item to data" % item)
        data = data.loc[:, ["dateTime", "securityId"] + real_items].copy(deep=True)
        connect.logout()
        return data

    def __get_adj_price(self, DataFrame, adjusted):
        data = DataFrame.copy(deep=True)
        data.sort_values(by="dateTime", axis=0, ascending=True, inplace=True)
        data.reset_index(inplace=True, drop=True)
        if adjusted == "F":
            adjfactor = data.loc[:, "adjfactor"][len(data) - 1]
        elif adjusted == "B":
            adjfactor = data.loc[:, "adjfactor"][0]
        else:
            raise ValueError("[%s] adjusted: %s did't support" % (self.LOGGER_NAME, adjusted))
        data.loc[:, "adjfactor"] = data.loc[:, "adjfactor"].apply(lambda x: x / adjfactor)
        columns = copy.deepcopy(self.__need_adjust_columns)
        for column in columns:
            data.loc[:, column] = data.loc[:, column] * data.loc[:, "adjfactor"]
        return data












