import tushare as ts
import abc
import six


@six.add_metaclass(abc.ABCMeta)
class BaseTuShare:

    def __init__(self):
        self.__parameters = {"token": "your tushare token here"}

    def set_parameter(self, parameter):
        if not isinstance(parameter, dict):
            raise BaseException("[BaseTuShare] the type of parameter is %s, not dict" % type(parameter))
        self.__parameters.update(parameter)

    def get_parameter(self, key):
        if not isinstance(key, str):
            raise BaseException("[BaseTuShare] the type of key is %s, not str" % type(key))
        if key not in self.__parameters.keys():
            raise BaseException("[BaseTuShare] the key not found in parameters, please use setparameter function")
        return self.__parameters.get(key)

    def connect_pro(self):
        token = self.get_parameter(key="token")
        connect = ts.pro_api(token)
        return connect

    @staticmethod
    def connect_ts(self):
        return ts

    def get_security_description(self, instruments):
        if not isinstance(instruments, list):
            raise BaseException("[BaseTuShare] the type of instruments must be list")
        for instrument in instruments:
            if not isinstance(instrument, str):
                raise BaseException("[BaseTuShare] the instrument in instruments must be str")
        # stock
        connect = self.connect_pro()
        data = connect.query(api_name="stock_basic")
        data.rename(columns={"ts_code": "securityId", "name": "securityName", "market": "securityMarket"}, inplace=True)
        data = data.loc[:, ["securityId", "securityName", "securityMarket"]].copy(deep=True)
        return data
