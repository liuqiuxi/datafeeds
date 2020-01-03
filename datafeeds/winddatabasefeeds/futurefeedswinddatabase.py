# -*- coding:utf-8 -*-
# @Time    : 2019-12-30 14:00
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : futurefeedswinddatabase.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of future market

import datetime
import pandas as pd
from datafeeds.winddatabasefeeds import BaseWindDataBase
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class AFutureQuotationWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AFutureQuotationWindDataBase"

    def __init__(self):
        super(AFutureQuotationWindDataBase, self).__init__()
        self.__table_name_dict = {"index_future": "CIndexFuturesEODPrices",
                                  "commodity_future": "CCommodityFuturesEODPrices",
                                  "bond_future": "CBondFuturesEODPrices"}

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
        if adjusted is not None:
            log = logger.get_logger(name=self.LOGGER_NAME)
            log.info("Future data has no adjust price, so we change adjusted to None")
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
        # index future
        table_name = self.__table_name_dict.get("index_future")
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        if len(securityIds) == 1:
            sqlClause = ("select * from " + table_parameter + " where trade_dt >= " +
                         "'" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' and s_info_windcode = " +
                         "'" + securityIds[0] + "'")
        else:
            sqlClause = ("select * from " + table_parameter + " where trade_dt >= " +
                         "'" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' and s_info_windcode in " +
                         "" + str(tuple(securityIds)) + "")
        data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        # commodity future
        table_name = self.__table_name_dict.get("commodity_future")
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        if frequency != 86400:
            raise BaseException("[%s] we can't supply frequency: %d " % (self.LOGGER_NAME, frequency))
        if len(securityIds) == 1:
            sqlClause = ("select * from " + table_parameter + " where trade_dt >= " +
                         "'" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' and s_info_windcode = " +
                         "'" + securityIds[0] + "'")
        else:
            sqlClause = ("select * from " + table_parameter + " where trade_dt >= " +
                         "'" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' and s_info_windcode in " +
                         "" + str(tuple(securityIds)) + "")
        data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        # index future did't have open interest change data, but commodity future may have
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
        # bond future
        table_name = self.__table_name_dict.get("bond_future")
        owner = self.get_oracle_owner(table_name=table_name)
        table_parameter = owner + table_name
        if len(securityIds) == 1:
            sqlClause = ("select * from " + table_parameter + " where trade_dt >= " +
                         "'" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' and s_info_windcode = " +
                         "'" + securityIds[0] + "'")
        else:
            sqlClause = ("select * from " + table_parameter + " where trade_dt >= " +
                         "'" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' and s_info_windcode in " +
                         "" + str(tuple(securityIds)) + "")
        data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        # bond future did't have open interest change data, but commodity future may have
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
        rename_dict = BarFeedConfig.get_wind_database_items().get(self.LOGGER_NAME)
        data.rename(columns=rename_dict, inplace=True)
        # change some parameters value to normal value
        data.loc[:, 'dateTime'] = data.loc[:, 'dateTime'].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data.loc[:, "amount"] = data.loc[:, "amount"] * 10000
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







