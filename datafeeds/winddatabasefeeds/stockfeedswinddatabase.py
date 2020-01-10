# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 16:11
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : stockfeedswinddatabase.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of stock market

import datetime
import copy
import pandas as pd
import numpy as np
from datafeeds.utils import BarFeedConfig
from datafeeds.winddatabasefeeds import BaseWindDataBase
from datafeeds import logger


class AShareCalendarWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AShareCalendarWindDataBase"

    def __init__(self):
        super(AShareCalendarWindDataBase, self).__init__()
        self.__table_name_dict = {"AShareCalendarWindDataBase": "AShareCalendar"}

    def get_calendar(self, begin_datetime, end_datetime):
        connect = self.connect()
        begin_datetime = begin_datetime.strftime("%Y%m%d")
        end_datetime = end_datetime.strftime("%Y%m%d")
        table_name = self.__table_name_dict.get(self.LOGGER_NAME)
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        sqlClause = ("select trade_days as dateTime from " + table_parameter + " where trade_days >= " +
                     "'" + begin_datetime + "' and trade_days <= '" + end_datetime + "' ")
        data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data.rename(columns={"datetime": "dateTime"}, inplace=True)
        data.drop_duplicates(subset=["dateTime"], inplace=True)
        data.sort_values(by="dateTime", inplace=True)
        data.reset_index(inplace=True, drop=True)
        data.loc[:, "dateTime"] = data.loc[:, "dateTime"].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data = pd.DataFrame(data={"dateTime": data.loc[:, "dateTime"]})
        connect.close()
        return data


class AShareQuotationWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AShareQuotationWindDataBase"

    def __init__(self):
        super(AShareQuotationWindDataBase, self).__init__()
        self.__need_adjust_columns = ["preClose", "open", "high", "low", "close", "volume", "avgPrice"]
        self.__table_name_dict = {"AShareQuotationWindDataBase": "AShareEODPrices"}

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted="F"):
        limit_numbers = BarFeedConfig.get_wind().get("LimitNumbers")
        if len(securityIds) < limit_numbers:
            data = self.__get_quotation(securityIds=securityIds, items=items, frequency=frequency,
                                        begin_datetime=begin_datetime, end_datetime=end_datetime, adjusted=adjusted)
        else:
            data = pd.DataFrame()
            for i in range(int(len(securityIds) / limit_numbers) + 1):
                data0 = self.__get_quotation(securityIds=securityIds[i*limit_numbers: i*limit_numbers + limit_numbers],
                                             items=items, frequency=frequency, begin_datetime=begin_datetime,
                                             end_datetime=end_datetime, adjusted=adjusted)
                data = pd.concat(objs=[data, data0], axis=0, join="outer")
        data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
        data.reset_index(inplace=True, drop=True)
        return data

    def __get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted):
        connect = self.connect()
        begin_datetime = begin_datetime.strftime("%Y%m%d")
        end_datetime = end_datetime.strftime("%Y%m%d")
        table_name = self.__table_name_dict.get(self.LOGGER_NAME)
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        if frequency != 86400:
            raise BaseException("[%s] we can't supply frequency: %d " % (self.LOGGER_NAME, frequency))
        if len(securityIds) == 1:
            sqlClause = ("select * from " + table_parameter + " where trade_dt >= '" + begin_datetime + "' " +
                         "and trade_dt <= '" + end_datetime + "' and s_info_windcode = '" + securityIds[0] + "'")
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
        # use adjfactor get adj price
        if adjusted in ["F", "B"]:
            data = data.groupby(by="securityId").apply(lambda x: self.__get_adj_price(DataFrame=x, adjusted=adjusted))
        data.reset_index(inplace=True, drop=True)
        data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
        data.reset_index(inplace=True, drop=True)
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
        connect.close()
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


class AShareIPOWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AShareIPOWindDataBase"

    def __init__(self):
        super(AShareIPOWindDataBase, self).__init__()
        self.__table_name_dict = {"AShareIPOWindDataBase": "AShareIPO"}

    def get_initial_public_offering(self, securityIds):
        connect = self.connect()
        table_name = self.__table_name_dict.get(self.LOGGER_NAME)
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        sqlClause = "select * from " + table_parameter + ""
        data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        default_items = list(BarFeedConfig.get_wind_database_items().get(self.LOGGER_NAME))
        drop_items = list(set(data.columns) - set(default_items))
        data.drop(labels=drop_items, axis=1, inplace=True)
        rename_dict = BarFeedConfig.get_wind_database_items().get(self.LOGGER_NAME)
        data.rename(columns=rename_dict, inplace=True)
        data0 = pd.DataFrame({"securityId": securityIds})
        data = pd.merge(left=data, right=data0, on="securityId", how="right")
        # change parameters numbers
        data.loc[:, "amount"] = data.loc[:, "amount"] * 10000
        data.loc[:, "collection"] = data.loc[:, "collection"] * 10000
        data.loc[:, "subDate"] = data.loc[:, "subDate"].apply(
            lambda x: datetime.datetime.strptime(x, "%Y%m%d") if isinstance(x, datetime.datetime) else None)
        data.loc[:, "listDate"] = data.loc[:, "listDate"].apply(
            lambda x: datetime.datetime.strptime(x, "%Y%m%d") if isinstance(x, datetime.datetime) else None)
        data.sort_values(by="securityId", axis=0, ascending=True, inplace=True)
        data.reset_index(inplace=True, drop=True)
        return data


class AShareDayVarsWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AShareDayVarsWindDataBase"

    def __init__(self):
        super(AShareDayVarsWindDataBase, self).__init__()
        self.__table_name_dict = {"AShareDayVarsWindDataBase": ["AShareEODDerivativeIndicator",
                                                                "AShareEODPrices",
                                                                "AShareST"]}

    def get_value(self, date_datetime):
        connect = self.connect()
        table_name = self.__table_name_dict.get(self.LOGGER_NAME)[0]
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        date_datetime = date_datetime.strftime("%Y%m%d")
        sqlClause = "select * from " + table_parameter + " where trade_dt = "+ date_datetime +""
        data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        default_items = list(BarFeedConfig.get_wind_database_items().get(self.LOGGER_NAME))
        drop_items = list(set(data.columns) - set(default_items))
        data.drop(labels=drop_items, axis=1, inplace=True)
        rename_dict = BarFeedConfig.get_wind_database_items().get(self.LOGGER_NAME)
        data.rename(columns=rename_dict, inplace=True)
        # change parameters numbers
        data.loc[:, "dateTime"] = data.loc[:, "dateTime"].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data.loc[:, "upLimit"] = np.where(data.loc[:, "upOrdown"] == 1, True, False)
        data.loc[:, "downLimit"] = np.where(data.loc[:, "upOrdown"] == -1, True, False)
        data.loc[:, "turnover"] = data.loc[:, "turnover"] / 100
        data.loc[:, "turnover_free"] = data.loc[:, "turnover_free"] / 100
        data.loc[:, "totalValue"] = data.loc[:, "totalValue"] * 10000
        data.loc[:, "marketValue"] = data.loc[:, "marketValue"] * 10000
        data.drop(labels=["upOrdown"], axis=1, inplace=True)
        # find stock whether suspend
        table_name = self.__table_name_dict.get(self.LOGGER_NAME)[1]
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        sqlClause = ("select s_info_windcode, trade_dt, s_dq_tradestatus from " + table_parameter + " " \
                    "where trade_dt = '"+ date_datetime +"'")
        data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data0.rename(columns=rename_dict, inplace=True)
        data0.loc[:, "dateTime"] = data0.loc[:, "dateTime"].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data0.loc[:, "isNotSuspended"] = np.where(data0.loc[:, "s_dq_tradestatus"] == "交易", True, False)
        data0 = data0.loc[:, ["securityId", "dateTime", "isNotSuspended"]].copy(deep=True)
        data = pd.merge(left=data, right=data0, how="outer", on=("dateTime", "securityId"))
        # find stock whether ST
        table_name = self.__table_name_dict.get(self.LOGGER_NAME)[2]
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        sqlClause = ("select s_info_windcode, entry_dt, remove_dt, s_type_st from " + table_parameter + " " \
                     "where entry_dt <= '" + date_datetime + "'")
        data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data0.rename(columns=rename_dict, inplace=True)
        data0.loc[:, "entry_dt"] = data0.loc[:, "entry_dt"].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data0.loc[:, "remove_dt"] = data0.loc[:, "remove_dt"].apply(
            lambda x: np.nan if x == None else datetime.datetime.strptime(x, "%Y%m%d"))
        date_datetime = datetime.datetime.strptime(date_datetime, "%Y%m%d")
        data0.loc[:, "isST"] = np.where(pd.isnull(data0.loc[:, "remove_dt"]), True,
                                        np.where(data0.loc[:, "remove_dt"] > date_datetime, True, False))
        data0 = data0.loc[data0.loc[:, "isST"] == True, ["securityId", "isST"]].copy(deep=True)
        data = pd.merge(left=data, right=data0, how="left", on="securityId")
        data.loc[:, "isST"] = np.where(data.loc[:, "isST"] == True, True, False)
        return data
























