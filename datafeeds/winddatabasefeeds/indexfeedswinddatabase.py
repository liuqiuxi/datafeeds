# -*- coding:utf-8 -*-
# @Time    : 2019-12-30 13:09
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : indexfeedswinddatabase.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of stock market

import datetime
import pandas as pd
from datafeeds.utils import BarFeedConfig
from datafeeds.winddatabasefeeds import BaseWindDataBase
from datafeeds import logger


class AIndexQuotationWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AIndexQuotationWindDataBase"

    def __init__(self):
        super(AIndexQuotationWindDataBase, self).__init__()
        self.__table_name_dict = {"AIndexQuotationWindDataBase": "AIndexEODPrices"}

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
        if adjusted is not None:
            log = logger.get_logger(name=self.LOGGER_NAME)
            log.info("index data has no adjust price, so we change adjusted to None")
        limit_numbers = BarFeedConfig.get_wind().get("LimitNumbers")
        if len(securityIds) < limit_numbers:
            data = self.__get_quotation(securityIds=securityIds, items=items, frequency=frequency,
                                        begin_datetime=begin_datetime, end_datetime=end_datetime)
        else:
            data = pd.DataFrame()
            for i in range(int(len(securityIds) / limit_numbers) + 1):
                data0 = self.__get_quotation(securityIds=securityIds[i*limit_numbers: i*limit_numbers + limit_numbers],
                                             items=items, frequency=frequency, begin_datetime=begin_datetime,
                                             end_datetime=end_datetime)
                data = pd.concat(objs=[data, data0], axis=0, join="outer")
        data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
        data.reset_index(inplace=True, drop=True)
        return data

    def __get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime):
        connect = self.connect()
        begin_datetime = begin_datetime.strftime("%Y%m%d")
        end_datetime = end_datetime.strftime("%Y%m%d")
        table_name = self.__table_name_dict.get(self.LOGGER_NAME)
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        if frequency != 86400:
            raise BaseException("[%s] we can't supply frequency: %d " % (self.LOGGER_NAME, frequency))
        if len(securityIds) == 1:
            sqlClause = ("select * from " + table_parameter + " where trade_dt >= '" + begin_datetime + "' and " +
                         "trade_dt <= '" + end_datetime + "' and s_info_windcode = '" + securityIds[0] + "'")
        else:
            sqlClause = ("select * from " + table_parameter + " where trade_dt >= '" + begin_datetime + "' and " +
                         "trade_dt <= '" + end_datetime + "' and s_info_windcode in " + str(tuple(securityIds)) + "")
        data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        rename_dict = BarFeedConfig.get_wind_database_items().get(self.LOGGER_NAME)
        data.rename(columns=rename_dict, inplace=True)
        # change some parameters value to normal value
        data.loc[:, 'dateTime'] = data.loc[:, 'dateTime'].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data.loc[:, "Chg"] = data.loc[:, "Chg"] / 100
        data.loc[:, "amount"] = data.loc[:, "amount"] * 1000
        # choose items to data
        log = logger.get_logger(name=self.LOGGER_NAME)
        default_items = BarFeedConfig.get_wind_database_items().get(self.LOGGER_NAME)
        default_items = list(default_items.values())
        real_items = []
        for item in items:
            if item in ["securityId", "dateTime"]:
                log.info("There is no need add item: %s to parameters items" % item)
            elif item in default_items:
                real_items.append(item)
            else:
                log.warning("item %s not in default items, so we remove this item to data" % item)
        data = data.loc[:, ["dateTime", "securityId"] + real_items].copy(deep=True)
        connect.close()
        return data


class AIndexWeightsWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AIndexWeightsWindDataBase"

    def __init__(self):
        super(AIndexWeightsWindDataBase, self).__init__()
        self.__rename_dict = {"s_con_windcode": "securityId", "trade_dt": "dateTime",
                              "i_weight": "securityWeight", "weight": "securityWeight"}
        self.__table_name_dict = {"000016.SH": "AIndexSSE50Weight", "000905.SH": "AIndexCSI500Weight",
                                  "000906.SH": "AIndexCSI800Weight", "000300.SH": "AIndexHS300Weight"}

    def get_index_weights(self, securityIds, date_datetime):
        data = pd.DataFrame()
        rename_dict = self.__rename_dict
        for securityId in securityIds:
            data0 = self.__get_index_weights(securityId=securityId, date_datetime=date_datetime)
            data0.rename(columns=rename_dict, inplace=True)
            data0 = data0.loc[:, ["dateTime", "indexId", "securityId", "securityWeight"]].copy(deep=True)
            data = pd.concat(objs=[data, data0], axis=0, join="outer")
        data.loc[:, "dateTime"] = data.loc[:, "dateTime"].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data.loc[:, "securityWeight"] = data.loc[:, "securityWeight"] / 100
        data = data.loc[:, ["dateTime", "indexId", "securityId", "securityWeight"]].copy(deep=True)
        data.sort_values(by=["indexId", "securityId"], axis=0, ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        return data

    def __get_index_weights(self, securityId, date_datetime):
        connect = self.connect()
        date_datetime = date_datetime.strftime("%Y%m%d")
        table_name_dict = self.__table_name_dict
        table_name = table_name_dict.get(securityId, None)
        if table_name is None:
            raise BaseException("[%s] can not support index: %s now" % (self.LOGGER_NAME, securityId))
        owner = self.get_oracle_owner(table_name=table_name)
        sqlClause = ("select * from " + owner + table_name + " where trade_dt = '" + date_datetime + "'")
        data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data.loc[:, "indexId"] = securityId
        connect.close()
        return data















