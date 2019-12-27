# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 15:48
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : stockfeedsjqdata.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of stock market

from datafeeds.jqdatafeeds import BaseJqData


class AShareCalendarJqData(BaseJqData):

    def __init__(self):
        super(AShareCalendarJqData, self).__init__()

    def get_calendar(self, begin_datetime, end_datetime):
        connect = self.connect()
        data = connect.get_trade_days(start_date=begin_datetime, end_date=end_datetime, count=None)