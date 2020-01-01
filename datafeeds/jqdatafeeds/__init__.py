# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 9:58
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : __init__.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is base jqdata class and can not used directly


import jqdatasdk
import abc
import copy
import pandas as pd
from datafeeds.utils import BarFeedConfig
from datafeeds import logger


class BaseJqData(metaclass=abc.ABCMeta):
    LOGGER_NAME = "BaseJqData"

    def __init__(self):
        self.__parameters = copy.deepcopy(BarFeedConfig.get_jqdata())
        self.__securityTypeList = {"stock": "AShare", "fund": "AFund", "index": "AIndex", "futures": "AFuture",
                                   "options": "AOption", "etf": "AFund", "lof": "AFund", "fja": "AFund", "fjb": "AFund",
                                   "open_fund": "AFund", "bond_fund": "AFund", "stock_fund": "AFund",
                                   "QDII_fund": "AFund", "money_market_fund": "AFund", "mixture_fund": "AFund"}
        self.__default_to_wind = {"XSHG": "SH", "XSHE": "SZ", "CCFX": "CFE", "XDCE": "DCE", "XSGE": "SHF",
                                  "XZCE": "CZC", "XINE": "INE"}
        self.__wind_to_default = {"SH": "XSHG", "SZ": "XSHE", "CFE": "CCFX", "DCE": "XDCE", "SHF": "XSGE",
                                  "CZC": "XZCE", "INE": "XINE"}

    def set_parameter(self, parameter):
        if not isinstance(parameter, dict):
            raise BaseException("[BaseJqData] the type of parameter is %s, not dict" % type(parameter))
        self.__parameters.update(parameter)

    def get_parameter(self, key):
        if not isinstance(key, str):
            raise BaseException("[BaseJqData] the type of key is %s, not str" % type(key))
        if key not in self.__parameters.keys():
            raise BaseException("[BaseJqData] the key not found in parameters, please use set_parameter function")
        return self.__parameters.get(key)

    def default_to_wind(self, securityIds):
        instruments = []
        transform_dict = copy.deepcopy(self.__default_to_wind)
        if not isinstance(securityIds, list):
            raise BaseException("[BaseTuShare] the type of securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[BaseTuShare] the securityId in securityIds must be str")
            key = securityId[securityId.find(".") + 1:]
            # 寻找主力连续合约
            if securityId[securityId.find(".") - 4: securityId.find(".")] == "9999":
                securityId = securityId.replace("9999", "")
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
            # 寻找主力连续合约
            if len(securityId[: securityId.find(".")]) < 3:
                length = securityId.find(".")
                securityId = securityId[:len(securityId[: length])] + "9999" + securityId[length:]
            if key in transform_dict.keys():
                instrument = securityId[:securityId.find(".") + 1] + transform_dict.get(key)
            else:
                instrument = securityId
            instruments.append(instrument)
        return instruments

    def connect(self):
        username = self.get_parameter(key="username")
        password = self.get_parameter(key="password")
        jqdatasdk.auth(username=username, password=password)
        if not jqdatasdk.is_auth():
            raise BaseException("[BaseJQData] not connect to jqData now, please try again later")
        spare = jqdatasdk.get_query_count().get("spare")
        total = jqdatasdk.get_query_count().get("total")
        if spare == 0:
            raise BaseException("[BaseJQData] spare number was zero, can not get more data")
        else:
            log = logger.get_logger(name=self.LOGGER_NAME)
            log.info("spare number was %d, data used %.2f" % (spare, 1 - spare/total))
        return jqdatasdk

    def get_security_type(self, securityIds):
        if not isinstance(securityIds, list):
            raise BaseException("[BaseJqData] the type of securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[BaseJqData] the securityId in securityIds must be str")
        connect = self.connect()
        jqdata_type_dict = copy.deepcopy(self.__securityTypeList)
        data = connect.get_all_securities(types=list(jqdata_type_dict.keys()))
        data.loc[:, "securityId"] = data.index
        data.loc[:, "securityType"] = data.loc[:, "type"].apply(lambda x: jqdata_type_dict.get(x))
        data.loc[:, "securityId"] = self.default_to_wind(securityIds=list(data.loc[:, "securityId"]))
        # transform column name to wind column name
        data.rename(columns={"display_name": "securityName"}, inplace=True)
        data = data.loc[:, ["securityId", "securityName", "securityType"]].copy(deep=True)
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
            raise BaseException("[BaseJqData] frequency type only can be int or float")
        if frequency <= 0:
            raise BaseException("[BaseJqData] frequency must great than 0")
        if int(frequency / 86400) == 0:
            cycle = str(int(frequency / 60)) + "m"
        else:
            cycle = str(int(frequency / 86400)) + "d"
        return cycle













