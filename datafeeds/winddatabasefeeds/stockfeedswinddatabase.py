# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 16:11
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : stockfeedswinddatabase.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of stock market

import datetime
import pandas as pd
from datafeeds.winddatabasefeeds import BaseWindDataBase


class AShareCalendarWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AShareCalendarWindDataBase"

    def __init__(self):
        super(AShareCalendarWindDataBase, self).__init__()

    def get_calendar(self, begin_datetime, end_datetime):
        connect = self.connect()
        begin_datetime = begin_datetime.strftime("%Y%m%d")
        end_datetime = end_datetime.strftime("%Y%m%d")
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = 'ASHARECALENDAR'"
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the ASHARECALENDAR table do not exist" % self.LOGGER_NAME)
        else:
            owner = owner.values()[0] + "."
        sqlClause = ("select trade_days as dateTime from " + owner + "AShareCalendar where trade_days >= " +
                     "'" + begin_datetime + "' and trade_days <= '" + end_datetime + "' ")
        data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data.rename(columns={"datetime": "dateTime"}, inplace=True)
        data.drop_duplicates(subset=["dateTime"], inplace=True)
        data.sort_values(by="dateTime", inplace=True)
        data.reset_index(inplace=True, drop=True)
        data.loc[:, "dateTime"] = data.loc[:, "dateTime"].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data = pd.DataFrame(data={"dateTime": data.loc[:, "dateTime"]})
        return data

