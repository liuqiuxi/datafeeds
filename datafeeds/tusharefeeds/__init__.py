import tushare as ts
import pandas as pd
import abc
import six


@six.add_metaclass(abc.ABCMeta)
class BaseTuShare:

    def __init__(self):
        self.__parameters = {"token": "your token",
                             "futureExchange": {"CFFEX": "CFE", "DCE": "DCE", "CZCE": "CZC", "SHFE": "SHF", "INE": "INE"},
                             "indexExchange": ["MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH"],
                             "fundExchange": ["E", "O"],
                             "optionExchange": {"CZCE": "CZC", "SHFE": "SHF", "DCE": "DCE", "SSE": "SH"}
                             }

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
    def connect_ts(self):
        return ts

    def get_security_type(self, instruments):
        if not isinstance(instruments, list):
            raise BaseException("[BaseTuShare] the type of instruments must be list")
        for instrument in instruments:
            if not isinstance(instrument, str):
                raise BaseException("[BaseTuShare] the instrument in instruments must be str")
        # stock
        connect = self.connect_pro()
        data = connect.query(api_name="stock_basic", fields="ts_code, name")
        data.loc[:, "securityType"] = "AShare"
        data = data.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        # future
        data0 = pd.DataFrame(columns=["ts_code", "name"])
        future_exchanges = self.get_parameter(key="futureExchange")
        for exchange in future_exchanges.keys():
            data1 = connect.query(api_name="fut_basic", exchange=exchange, fields="ts_code, name")
            name = future_exchanges.get(exchange)
            data1.loc[:, "ts_code"] = data1.loc[:, "ts_code"].apply(lambda x: x[:x.find(".")] + "." + name)
            data0 = pd.concat(objs=[data0, data1], axis=0, join="outer", sort=False)
        data0.reset_index(drop=True, inplace=True)
        data0.loc[:, "securityType"] = "AFuture"
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # index
        index_exchanges = self.get_parameter(key="indexExchange")
        data0 = pd.DataFrame(columns=["ts_code", "name"])
        for exchange in index_exchanges:
            data1 = connect.query(api_name="index_basic", market=exchange, fields="ts_code, name")
            data0 = pd.concat(objs=[data0, data1], axis=0, join="outer", sort=False)
        data0.reset_index(drop=True, inplace=True)
        data0.loc[:, "securityType"] = "AIndex"
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # fund
        fund_exchanges = self.get_parameter(key="fundExchange")
        data0 = pd.DataFrame(columns=["ts_code", "name"])
        for exchange in fund_exchanges:
            data1 = connect.query(api_name="fund_basic", market=exchange, fields="ts_code, name")
            data0 = pd.concat(objs=[data0, data1], axis=0, join="outer", sort=False)
        data0.reset_index(drop=True, inplace=True)
        data0.loc[:, "securityType"] = "AFund"
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # option
        option_exchanges = self.get_parameter(key="optionExchange")
        data0 = pd.DataFrame(columns=["ts_code", "name"])
        for exchange in option_exchanges.keys():
            data1 = connect.query(api_name="opt_basic", exchange=exchange, fields="ts_code, name")
            name = option_exchanges.get(exchange)
            data1.loc[:, "ts_code"] = data1.loc[:, "ts_code"].apply(lambda x: x[:x.find(".")] + "." + name)
            data0 = pd.concat(objs=[data0, data1], axis=0, join="outer", sort=False)
        data0.loc[:, "securityType"] = "AOption"
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # bond
        data0 = connect.query(api_name="cb_basic", fields="ts_code, bond_short_name")
        data0.loc[:, "securityType"] = "ABond"
        data0.rename(columns={"bond_short_name": "name"}, inplace=True)
        data0 = data0.loc[:, ["ts_code", "name", "securityType"]].copy(deep=True)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        data.rename(columns={"ts_code": "securityId", "name": "securityName"}, inplace=True)
        data0 = pd.DataFrame(data={"securityId": instruments})
        data = pd.merge(left=data, right=data0, how="right", on="securityId")
        data.reset_index(drop=True, inplace=True)
        return data



