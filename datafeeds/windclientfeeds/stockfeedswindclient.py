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
        connect.stop()
        return data


class AShareIndustryWindClient(BaseWindClient):
    LOGGER_NAME = "AShareIndustryWindClient"

    def __init__(self):
        super(AShareIndustryWindClient, self).__init__()

    def get_sw_industry(self, securityIds, date_datetime, lv):
        connect = self.connect()
        date_datetime = date_datetime.strftime("%Y%m%d")
        lv = str(lv)
        instruments = ""
        for securityId in securityIds:
            instruments = securityId + "," + instruments
        instruments = instruments[:-1]
        data = connect.wss(codes=instruments, fields="industry2",
                           options="industryType=1;industryStandard=" + lv + ";tradeDate=" + date_datetime + "")
        data = pd.DataFrame(data={"securityId": data.Codes, "industryName": data.Data[0]})
        data.loc[:, "dateTime"] = datetime.datetime.strptime(date_datetime, "%Y%m%d")
        connect.stop()
        return data

    def get_zx_industry(self, securityIds, date_datetime, lv):
        connect = self.connect()
        date_datetime = date_datetime.strftime("%Y%m%d")
        lv = str(lv)
        instruments = ""
        for securityId in securityIds:
            instruments = securityId + "," + instruments
        instruments = instruments[:-1]
        data = connect.wss(codes=instruments, fields="industry2",
                           options="industryType=3;industryStandard=" + lv + ";tradeDate=" + date_datetime + "")
        data = pd.DataFrame(data={"securityId": data.Codes, "industryName": data.Data[0]})
        data.loc[:, "dateTime"] = datetime.datetime.strptime(date_datetime, "%Y%m%d")
        connect.stop()
        return data

    def get_wind_industry(self, securityIds, date_datetime, lv):
        connect = self.connect()
        date_datetime = date_datetime.strftime("%Y%m%d")
        lv = str(lv)
        instruments = ""
        for securityId in securityIds:
            instruments = securityId + "," + instruments
        instruments = instruments[:-1]
        data = connect.wss(codes=instruments, fields="industry2",
                           options="industryType=2;industryStandard=" + lv + ";tradeDate=" + date_datetime + "")
        data = pd.DataFrame(data={"securityId": data.Codes, "industryName": data.Data[0]})
        data.loc[:, "dateTime"] = datetime.datetime.strptime(date_datetime, "%Y%m%d")
        connect.stop()
        return data



