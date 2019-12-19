import abc
import six
import jqdatasdk
import logging


@six.add_metaclass(abc.ABCMeta)
class BaseJQData:

    def __init__(self):
        self.__parameters = {"username": "your username",
                             "password": "your password"}

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

    def connect(self):
        username = self.get_parameter(key="username")
        password = self.get_parameter(key="password")
        jqdatasdk.auth(username=username, password=password)
        if not jqdatasdk.is_auth():
            raise BaseException("[BaseJQData] not connect to jqData, please try again later")
        count = jqdatasdk.get_query_count().get("spare")
        if count == 0:
            raise BaseException("[BaseJQData] spare number was zero, can not get more data")
        else:
            logging.info("[BaseJQData] spare number was %d" % count)
        return jqdatasdk




