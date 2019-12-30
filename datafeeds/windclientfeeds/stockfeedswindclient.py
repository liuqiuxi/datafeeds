# -*- coding:utf-8 -*-
# @Time    : 2019-12-30 9:42
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : stockfeedswindclient.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of stock market

import datetime
import pandas as pd
from datafeeds.windclientfeeds import BaseWindClient


class AShareCalendarWindClient(BaseWindClient):

    def __init__(self):
        super(AShareCalendarWindClient, self).__init__()

    def get_calendar(self, begin_datetime, end_datetime):
        connect = self.connect()
        begin_datetime = begin_datetime.strftime("%Y-%m-%d")
        end_datetime = end_datetime.strftime("%Y-%m-%d")
        data = connect.tdays(beginTime=begin_datetime, endTime=end_datetime)
        data = pd.DataFrame(data={"dateTime": data.Data[0]})
        data.sort_values(by="dateTime", axis=0, ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        return data
