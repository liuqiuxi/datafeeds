# -*- coding:utf-8 -*-
# @Time    : 2019-12-30 13:09
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : indexfeedswinddatabase.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of stock market

import datetime
import copy
import pandas as pd
from datafeeds.utils import BarFeedConfig
from datafeeds.winddatabasefeeds import BaseWindDataBase
from datafeeds import logger


class AIndexQuotationWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AIndexQuotationWindDataBase"

    def __init__(self):
        super(AIndexQuotationWindDataBase, self).__init__()

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
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = 'AINDEXEODPRICES'"
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the AINDEXEODPRICES table do not exist" % self.LOGGER_NAME)
        else:
            owner = owner.values()[0] + "."
        if frequency != 86400:
            raise BaseException("[%s] we can't supply frequency: %d " % (self.LOGGER_NAME, frequency))
        if len(securityIds) == 1:
            sqlClause = ("select * from " + owner + "AIndexEODPrices where trade_dt >= '" + begin_datetime + "' and " +
                         "trade_dt <= '" + end_datetime + "' and s_info_windcode = '" + securityIds[0] + "'")
            # sqlClause = ("select s_info_windcode as securityId, trade_dt as dateTime, s_dq_preclose as preClose, " +
            #              "s_dq_open as open, s_dq_high as high, s_dq_low as low, s_dq_close as close, s_dq_change as " +
            #              "cash_change, s_dq_pctchange as Chg, s_dq_volume as volume, s_dq_amount as amount " +
            #              "from " + owner + "AIndexEODPrices where trade_dt >= '" + begin_datetime + "' and " +
            #              "trade_dt <= '" + end_datetime + "' and s_info_windcode = '" + securityIds[0] + "'")
        else:
            sqlClause = ("select * from " + owner + "AIndexEODPrices where trade_dt >= '" + begin_datetime + "' and " +
                         "trade_dt <= '" + end_datetime + "' and s_info_windcode in " + str(tuple(securityIds)) + "")
            # sqlClause = ("select s_info_windcode as securityId, trade_dt as dateTime, s_dq_preclose as preClose, " +
            #              "s_dq_open as open, s_dq_high as high, s_dq_low as low, s_dq_close as close, s_dq_change as " +
            #              "cash_change, s_dq_pctchange as Chg, s_dq_volume as volume, s_dq_amount as amount " +
            #              "from " + owner + "AIndexEODPrices where trade_dt >= '" + begin_datetime + "' and " +
            #              "trade_dt <= '" + end_datetime + "' and s_info_windcode in " + str(tuple(securityIds)) + "")
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






