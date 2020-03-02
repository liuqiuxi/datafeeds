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
import numpy as np
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


class AShareDayVarsJqData(BaseJqData):

    def __init__(self):
        super(AShareDayVarsJqData, self).__init__()

    def get_value(self, date_datetime):
        connect = self.connect()
        data = connect.get_all_securities(types=["stock"], date=date_datetime)
        data.loc[:, "securityId"] = self.default_to_wind(securityIds=list(data.index))
        data = data.loc[:, ["securityId", "start_date"]].copy(deep=True)
        data.loc[:, "code"] = data.index
        valuation = connect.valuation
        query = connect.query(valuation.code, valuation.market_cap, valuation.circulating_market_cap,
                              valuation.pe_ratio, valuation.turnover_ratio, valuation.pb_ratio
                              ).filter(connect.valuation.code.in_(list(data.index)))
        data0 = connect.get_fundamentals(query_object=query, date=date_datetime)
        data = pd.merge(left=data, right=data0, how="left", on="code")
        # 市值和总市值转化为元
        data.loc[:, "circulating_market_cap"] = data.loc[:, "circulating_market_cap"] * 100000000
        data.loc[:, "market_cap"] = data.loc[:, "market_cap"] * 100000000
        # 换手率转化为基本单位
        data.loc[:, "turnover_ratio"] = data.loc[:, "turnover_ratio"] / 100
        # jQDATA暂时未有ST和停牌数据，以后有了再补充
        data.rename(columns={"start_date": "listDate", "market_cap": "totalValue", "pe_ratio": "PE_TTM",
                             "pb_ratio": "PB", "circulating_market_cap": "marketValue", "turnover_ratio": "turnover"},
                    inplace=True)
        data.loc[:, "dateTime"] = date_datetime
        data = data.loc[:, ["dateTime", "securityId", "marketValue", "totalValue",
                            "turnover", "PE_TTM", "PB", "listDate"]].copy(deep=True)
        connect.logout()
        return data


class AShareIndustryJqData(BaseJqData):

    def __init__(self):
        super(AShareIndustryJqData, self).__init__()

    def get_sw_industry(self, securityIds, date_datetime, lv):
        connect = self.connect()
        securityIds = self.wind_to_default(securityIds=securityIds)
        data0 = connect.get_industry(security=securityIds, date=date_datetime)
        data = pd.DataFrame(data={"securityId": securityIds})
        # get SW key level
        key = "sw_l" + str(lv)
        data.loc[:, "industryName"] = data.loc[:, "securityId"].apply(lambda x: data0.get(x, np.nan))
        data.loc[:, "industryName"] = data.loc[:, "industryName"].apply(
            lambda x: x.get(key) if isinstance(x, dict) else np.nan)
        data.loc[:, "industryName"] = data.loc[:, "industryName"].apply(
            lambda x: x.get("industry_name", np.nan) if isinstance(x, dict) else np.nan)
        data.loc[:, "securityId"] = self.default_to_wind(securityIds=list(data.loc[:, "securityId"]))
        data.loc[:, "dateTime"] = date_datetime
        connect.logout()
        return data



















