# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 16:42
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : __init__.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is datafeeds client, the most important class

import pandas as pd
from datafeeds import logger
from datafeeds.client.fundfeeds import AFundQuotation
from datafeeds.client.futurefeeds import AFutureQuotation
from datafeeds.client.indexfeeds import AIndexQuotation
from datafeeds.client.optionfeeds import AOptionQuotation
from datafeeds.client.stockfeeds import AShareQuotation

from datafeeds.winddatabasefeeds import BaseWindDataBase as BaseSecurityType


class Quotation:

    @staticmethod
    def get_quotation(securityIds, items, frequency, begin_datetime, end_datetime, adjusted="B", dataSource=None):
        function_dict = {"AIndex": AIndexQuotation, "AShare": AShareQuotation, "AFuture": AFutureQuotation,
                         "AOption": AOptionQuotation, "AFund": AFundQuotation}
        data = BaseSecurityType().get_security_type(securityIds)
        data.dropna(inplace=True)
        if len(data) != len(securityIds):
            log = logger.get_logger(name="Quotation")
            non_find_securityIds = list(set(securityIds) - set(data.loc[:, "securityId"]))
            log.warning("the securityId: %s can not find type" % non_find_securityIds)
        security_type_list = list(set(data.loc[:, "securityType"]))
        data0 = pd.DataFrame(columns=["dateTime", "securityId"] + items)
        for security_type in security_type_list:
            function_ = function_dict.get(security_type)
            securityIds = list(set(data.loc[data.loc[:, "securityType"] == security_type, "securityId"]))
            data1 = function_.get_quotation(securityIds=securityIds, items=items, frequency=frequency,
                                            begin_datetime=begin_datetime, end_datetime=end_datetime,
                                            adjusted=adjusted, dataSource=dataSource)
            for column in data0.columns:
                if column not in data1.columns:
                    data1.loc[:, column] = None
            data1 = data1.loc[:, data0.columns].copy(deep=True)
            data0 = pd.concat(objs=[data0, data1], axis=0, join="outer")
        data0.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
        data0.reset_index(drop=True, inplace=True)
        return data0





