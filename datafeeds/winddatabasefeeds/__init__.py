# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 13:22
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : __init__.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is base tushare class and can not used directly

import sqlalchemy
import abc
import copy
import os
import pandas as pd
from datafeeds.utils import BarFeedConfig
from datafeeds import logger
os.environ["NLS_LANG"] = "GERMAN_GERMANY.UTF8"


class BaseWindDataBase(metaclass=abc.ABCMeta):
    LOGGER_NAME = "BaseWindDataBase"

    def __init__(self):
        self.__parameters = copy.deepcopy(BarFeedConfig.get_wind())

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

    def connect(self):
        userId = self.get_parameter(key="UserId")
        password = self.get_parameter(key="PassWord")
        server = self.get_parameter(key="Server")
        port = self.get_parameter(key="Port")
        database = self.get_parameter(key="DateBase")
        connectClause = "oracle://" + userId + ":" + password + "@" + server + ":" + port + "/" + database + ""
        connect = sqlalchemy.create_engine(connectClause).connect()
        return connect

    def get_oracle_owner(self, table_name):
        connect = self.connect()
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = '%s'" % table_name.upper()
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the %s table do not exist" % (self.LOGGER_NAME, table_name))
        else:
            owner = owner.values()[0] + "."
        connect.close()
        return owner

    @staticmethod
    def get_data_with_sql(sqlClause, connect):
        data = pd.read_sql(sql=sqlClause, con=connect)
        return data

    def get_security_type(self, securityIds):
        if not isinstance(securityIds, list):
            raise BaseException("[BaseJqData] the type of securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[BaseJqData] the securityId in securityIds must be str")
        connect = self.connect()
        # stock
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = 'ASHAREDESCRIPTION'"
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the ASHAREDESCRIPTION table do not exist" % self.LOGGER_NAME)
        else:
            owner = owner.values()[0] + "."
        sqlClause = ("select s_info_windcode as securityId, s_info_name as securityName from " +
                     "%sAShareDescription" % owner)
        data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data.loc[:, "securityType"] = "AShare"
        # future
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = 'CFUTURESDESCRIPTION'"
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the CFUTURESDESCRIPTION table do not exist" % self.LOGGER_NAME)
        else:
            owner = owner.values()[0] + "."
        sqlClause = ("select s_info_windcode as securityId, s_info_fullname as securityName from " +
                     "%sCFuturesDescription" % owner)
        data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data0.loc[:, "securityType"] = "AFuture"
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # index
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = 'AINDEXDESCRIPTION'"
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the AINDEXDESCRIPTION table do not exist" % self.LOGGER_NAME)
        else:
            owner = owner.values()[0] + "."
        sqlClause = ("select s_info_windcode as securityId, s_info_compname as securityName from " +
                     "%sAIndexDescription" % owner)
        data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data0.loc[:, "securityType"] = "AIndex"
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # fund
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = 'CHINAMUTUALFUNDDESCRIPTION'"
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the CHINAMUTUALFUNDDESCRIPTION table do not exist" % self.LOGGER_NAME)
        else:
            owner = owner.values()[0] + "."
        sqlClause = ("select f_info_windcode as securityId, f_info_fullname as securityName from " +
                     "%sChinaMutualFundDescription" % owner)
        data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data0.loc[:, "securityType"] = "AFund"
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # option
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = 'CHINAOPTIONDESCRIPTION'"
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the CHINAOPTIONDESCRIPTION table do not exist" % self.LOGGER_NAME)
        else:
            owner = owner.values()[0] + "."
        sqlClause = ("select s_info_windcode as securityId, s_info_name as securityName from " +
                     "%sChinaOptionDescription" % owner)
        data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data0.loc[:, "securityType"] = "AOption"
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # bond
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = 'CBONDDESCRIPTION'"
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the CBONDDESCRIPTION table do not exist" % self.LOGGER_NAME)
        else:
            owner = owner.values()[0] + "."
        sqlClause = ("select s_info_windcode as securityId, b_info_fullname as securityName from " +
                     "%sCBondDescription" % owner)
        data0 = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)
        data = pd.concat(objs=[data, data0], axis=0, join="outer", sort=False)
        # transform column name to wind column name
        data.rename(columns={"securityid": "securityId", "securityname": "securityName"}, inplace=True)
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
        # close database connect
        connect.close()
        return data











