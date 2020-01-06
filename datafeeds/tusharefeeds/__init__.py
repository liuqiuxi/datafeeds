# -*- coding:utf-8 -*-
# @Time    : 2019-12-26 17:05
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : __init__.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is base tushare class and can not used directly

import tushare as ts
import pandas as pd
import abc
import copy
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class BaseTuShare(metaclass=abc.ABCMeta):
    LOGGER_NAME = "BaseTuShare"

    def __init__(self):
        self.__parameters = copy.deepcopy(BarFeedConfig.get_tushare())
        self.__default_to_wind = {"CFX": "CFE", "ZCE": "CZC"}
        self.__wind_to_default = {"CFE": "CFX", "CZC": "ZCE"}

    def set_parameter(self, parameter):
        if not isinstance(parameter, dict):
            raise BaseException("[BaseTuShare] the type of parameter is %s, not dict" % type(parameter))
        self.__parameters.update(parameter)

    def get_parameter(self, key):
        if not isinstance(key, str):
            raise BaseException("[BaseTuShare] the type of key is %s, not str" % type(key))
        if key not in self.__parameters.keys():
            raise BaseException("[BaseTuShare] the key not found in parameters, please use set_parameter function")
        return self.__parameters.get(key)

    def connect_pro(self):
        token = self.get_parameter(key="token")
        connect = ts.pro_api(token)
        return connect

    @staticmethod
    def connect_ts():
        return ts

    def default_to_wind(self, securityIds):
        instruments = []
        transform_dict = copy.deepcopy(self.__default_to_wind)
        if not isinstance(securityIds, list):
            raise BaseException("[BaseTuShare] the type of securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[BaseTuShare] the securityId in securityIds must be str")
            key = securityId[securityId.find(".") + 1:]
            if key in transform_dict.keys():
                instrument = securityId[:securityId.find(".") + 1] + transform_dict.get(key)
            else:
                instrument = securityId
            instruments.append(instrument)
        return instruments

    def wind_to_default(self, securityIds):
        instruments = []
        transform_dict = copy.deepcopy(self.__wind_to_default)
        if not isinstance(securityIds, list):
            raise BaseException("[BaseTuShare] the type of securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[BaseTuShare] the securityId in securityIds must be str")
            key = securityId[securityId.find(".") + 1:]
            if key in transform_dict.keys():
                instrument = securityId[:securityId.find(".") + 1] + transform_dict.get(key)
            else:
                instrument = securityId
            instruments.append(instrument)
        return instruments

    def get_security_type(self, securityIds):
        if not isinstance(securityIds, list):
            raise BaseException("[BaseTuShare] the type of securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[BaseTuShare] the securityId in securityIds must be str")
        connect = self.connect_pro()
        # stock
        data = connect.query(api_name="stock_basic", fields="ts_code, name")
        data.loc[:, "securityType"] = "AShare"
        data = data.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        # future
        data0 = pd.DataFrame(columns=["ts_code", "name"])
        exchange_market = self.get_parameter(key="futureExchange")
        for exchange in exchange_market:
            data1 = connect.query(api_name="fut_basic", exchange=exchange, fields="ts_code, name")
            data0 = pd.concat(objs=[data0, data1], axis=0, join="outer", sort=False)
        data0.reset_index(drop=True, inplace=True)
        data0.loc[:, "securityType"] = "AFuture"
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # index
        exchange_market = self.get_parameter(key="indexExchange")
        data0 = pd.DataFrame(columns=["ts_code", "name"])
        for exchange in exchange_market:
            data1 = connect.query(api_name="index_basic", market=exchange, fields="ts_code, name")
            data0 = pd.concat(objs=[data0, data1], axis=0, join="outer", sort=False)
        data0.reset_index(drop=True, inplace=True)
        data0.loc[:, "securityType"] = "AIndex"
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # fund
        exchange_market = self.get_parameter(key="fundExchange")
        data0 = pd.DataFrame(columns=["ts_code", "name"])
        for exchange in exchange_market:
            data1 = connect.query(api_name="fund_basic", market=exchange, fields="ts_code, name")
            data0 = pd.concat(objs=[data0, data1], axis=0, join="outer", sort=False)
        data0.reset_index(drop=True, inplace=True)
        data0.loc[:, "securityType"] = "AFund"
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # option
        exchange_market = self.get_parameter(key="optionExchange")
        data0 = pd.DataFrame(columns=["ts_code", "name"])
        for exchange in exchange_market:
            data1 = connect.query(api_name="opt_basic", exchange=exchange, fields="ts_code, name")
            data0 = pd.concat(objs=[data0, data1], axis=0, join="outer", sort=False)
        data0.reset_index(drop=True, inplace=True)
        data0.loc[:, "securityType"] = "AOption"
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # bond
        data0 = connect.query(api_name="cb_basic", fields="ts_code, bond_short_name")
        data0.loc[:, "securityType"] = "ABond"
        data0.rename(columns={"bond_short_name": "name"}, inplace=True)
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # transform column name to wind column name
        data.rename(columns={"ts_code": "securityId", "name": "securityName"}, inplace=True)
        # transform default code to wind code
        data.loc[:, "securityId"] = self.default_to_wind(securityIds=list(data.loc[:, "securityId"]))
        data0 = pd.DataFrame(data={"securityId": securityIds})
        data = pd.merge(left=data, right=data0, how="right", on="securityId")
        data.sort_values(by="securityId", ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        # test whether has security did't find
        data0 = data.dropna(inplace=False)
        if len(data) != len(data0):
            log = logger.get_logger(name=self.LOGGER_NAME)
            non_find_securityId = set(data.loc[:, "securityId"]) - set(data0.loc[:, "securityId"])
            log.info("the securityId: %s can not find in data source" % non_find_securityId)
            data.dropna(inplace=True)
            data.reset_index(drop=True, inplace=True)
        return data

    @staticmethod
    def get_frequency_cycle(frequency):
        if not isinstance(frequency, (int, float)):
            raise BaseException("[BaseTuShare] frequency type only can be int or float")
        if frequency not in [60, 300, 900, 1800, 3600, 86400, 604800, 2592000]:
            raise BaseException("[BaseTuShare] frequency not support %d frequency" % frequency)
        if frequency == 86400:
            cycle = "D"
        elif frequency < 86400:
            cycle = str(int(frequency / 60)) + "min"
        elif frequency == 604800:
            cycle = "W"
        elif frequency == 2592000:
            cycle = "M"
        else:
            raise BaseException("[BaseTuShare] something may have problem")
        return cycle















