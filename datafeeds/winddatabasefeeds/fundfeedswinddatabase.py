# -*- coding:utf-8 -*-
# @Time    : 2019-12-31 10:40
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : fundfeedswinddatabase.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of fund market

import datetime
import copy
import pandas as pd
from datafeeds.winddatabasefeeds import BaseWindDataBase
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class AFundQuotationWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AFundQuotationWindDataBase"

    def __init__(self):
        super(AFundQuotationWindDataBase, self).__init__()
        self.__need_adjust_columns = ["preClose", "open", "high", "low", "close", "volume"]
        self.__table_name_dict = {"securityIds_OTC": "ChinaMutualFundNAV",
                                  "securityIds_EXC": "ChinaClosedFundEODPrice",
                                  "securityIds_OFO": "ChinaMutualFundNAV"}

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
        limit_numbers = BarFeedConfig.get_wind().get("LimitNumbers")
        if len(securityIds) < limit_numbers:
            data = self.__get_quotation(securityIds=securityIds, items=items, frequency=frequency,
                                        begin_datetime=begin_datetime, end_datetime=end_datetime, adjusted=adjusted)
        else:
            data = pd.DataFrame()
            for i in range(int(len(securityIds) / limit_numbers) + 1):
                data0 = self.__get_quotation(
                    securityIds=securityIds[i * limit_numbers: i * limit_numbers + limit_numbers],
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
        rename_dict = BarFeedConfig.get_wind_database_items().get(self.LOGGER_NAME)
        if frequency != 86400:
            raise BaseException("[%s] we can't supply frequency: %d " % (self.LOGGER_NAME, frequency))
        # because some code both in OTC market and exchange market
        # like 510050.SH means 510050.SH in exchange market and 510050.OF in OTC market
        # we separate then to OTC market and exchange market
        securityIds_OTC = []
        securityIds_EXC = []
        for securityId in securityIds:
            if securityId[securityId.find(".") + 1:] == "OF":
                securityIds_OTC.append(securityId)
            else:
                securityIds_EXC.append(securityId)
        # fund in exchange market
        table_name = self.__table_name_dict.get("securityIds_EXC")
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        if len(securityIds_EXC) > 0:
            if len(securityIds_EXC) == 1:
                sqlClause = ("select * from " + table_parameter + " where trade_dt >= " +
                             "'" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' and s_info_windcode = " +
                             "'" + securityIds_EXC[0] + "'")
            else:
                sqlClause = ("select * from " + table_parameter + " where trade_dt >= " +
                             "'" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' and s_info_windcode in " +
                             "" + str(tuple(securityIds_EXC)) + "")
            data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
            data.rename(columns=rename_dict, inplace=True)
        else:
            data = pd.DataFrame()
        # fund in OTC market
        table_name = self.__table_name_dict.get("securityIds_OTC")
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        if len(securityIds_OTC) > 0:
            if len(securityIds_OTC) == 1:
                sqlClause = ("select * from " + table_parameter + " where price_date >= " +
                             "'" + begin_datetime + "' and price_date <= '" + end_datetime + "' and " +
                             " f_info_windcode = '" + securityIds_OTC[0] + "'")
            else:
                sqlClause = ("select * from " + table_parameter + " where price_date >= " +
                             "'" + begin_datetime + "' and price_date <= '" + end_datetime + "' and " +
                             "f_info_windcode in " + str(tuple(securityIds_OTC)) + "")
            data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
            data0.rename(columns=rename_dict, inplace=True)
        else:
            data0 = pd.DataFrame()
        # some OTC fund like 510050.OF in OTC market code is 510050.SH, change it's code
        securityIds_OFO = []
        for securityId in securityIds_OTC:
            if securityId[0] == "5":
                securityId = securityId.replace("OF", "SH")
            elif securityId[0] == "1":
                securityId = securityId.replace("OF", "SZ")
            else:
                raise BaseException("[AFundQuotationWindDataBase] the fund code can't change to exchange market code")
            securityIds_OFO.append(securityId)
        table_name = self.__table_name_dict.get("securityIds_OFO")
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        if len(securityIds_OFO) > 0:
            if len(securityIds_OFO) == 1:
                sqlClause = ("select * from " + table_parameter + " where price_date >= " +
                             "'" + begin_datetime + "' and price_date <= '" + end_datetime + "' and " +
                             "f_info_windcode = '" + securityIds_OFO[0] + "'")
            else:
                sqlClause = ("select * from " + table_parameter + " where price_date >= " +
                             "'" + begin_datetime + "' and price_date <= '" + end_datetime + "' and " +
                             "f_info_windcode in " + str(tuple(securityIds_OFO)) + "")
            data1 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
            data1.rename(columns=rename_dict, inplace=True)
            # change code to OTC
            data1.loc[:, "securityId"] = data1.loc[:, "securityId"].apply(lambda x: x[:x.find(".") + 1] + "OF")
        else:
            data1 = pd.DataFrame()
        data0 = pd.concat(objs=[data0, data1], axis=0, join="outer")
        # OTC market fund and exchange market fund did't have same parameters
        if not data.empty and not data0.empty:
            columns = list(set(data.columns).union(set(data0.columns)))
            for column in columns:
                if column not in data.columns:
                    data.loc[:, column] = None
                if column not in data0.columns:
                    data0.loc[:, column] = None
            data = data.loc[:, columns].copy(deep=True)
            data0 = data0.loc[:, columns].copy(deep=True)
            data = pd.concat(objs=[data, data0], axis=0, join="outer")
        else:
            if data.empty:
                data = data0.copy(deep=True)
            elif data0.empty:
                data = data.copy(deep=True)
            else:
                raise BaseException("[AFutureQuotationWindDataBase] something may wrong")
        # change some parameters value to normal value
        data.loc[:, 'dateTime'] = data.loc[:, 'dateTime'].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        # use adjfactor get adj price
        if adjusted in ["F", "B"]:
            data = data.groupby(by="securityId").apply(lambda x: self.__get_adj_price(DataFrame=x, adjusted=adjusted))
        data.reset_index(inplace=True, drop=True)
        data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
        data.reset_index(inplace=True, drop=True)
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
            if column in data.columns:
                data.loc[:, column] = data.loc[:, column] * data.loc[:, "adjfactor"]
        return data






