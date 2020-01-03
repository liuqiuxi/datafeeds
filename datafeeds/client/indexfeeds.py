# -*- coding:utf-8 -*-
# @Time    : 2019-12-30 13:41
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : indexfeeds.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is all data source index feeds


import datetime
from datafeeds.utils import BarFeedConfig
from datafeeds import logger
from datafeeds.winddatabasefeeds.indexfeedswinddatabase import AIndexQuotationWindDataBase
from datafeeds.winddatabasefeeds.indexfeedswinddatabase import AIndexWeightsWindDataBase
from datafeeds.jqdatafeeds.indexfeedsjqdata import AIndexQuotationJqData


class AIndexQuotation:

    @staticmethod
    def get_quotation(securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None, dataSource=None):
        if not isinstance(securityIds, list):
            raise BaseException("[AIndexQuotation] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AIndexQuotation] securityId in securityIds must be str")
        if not isinstance(items, list):
            raise BaseException("[AIndexQuotation] items must be list")
        for item in items:
            if not isinstance(item, str):
                raise BaseException("[AIndexQuotation] item in items must be str")
        if not isinstance(begin_datetime, datetime.datetime):
            raise BaseException("[AIndexQuotation] begin_datetime must be datetime.datetime")
        if not isinstance(end_datetime, datetime.datetime):
            raise BaseException("[AIndexQuotation] end_datetime must be datetime.datetime")
        if begin_datetime > end_datetime:
            raise BaseException("[AIndexQuotation] begin_datetime must less than end_datetime")
        if adjusted not in ["F", "B", None]:
            raise BaseException("[AIndexQuotation] adjusted only can in [F, B, None], not supply %s" % adjusted)
        if dataSource is None:
            log = logger.get_logger(name="AIndexQuotation")
            dataSource = BarFeedConfig.get_client_config().get("AIndexQuotation")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            if frequency != 86400:
                raise BaseException("[AIndexQuotation] wind can not supply frequency: %d now" % frequency)
            data = AIndexQuotationWindDataBase().get_quotation(securityIds=securityIds, items=items,
                                                               frequency=frequency, begin_datetime=begin_datetime,
                                                               end_datetime=end_datetime, adjusted=adjusted)
        elif dataSource == "jqdata":
            if frequency % 60 != 0:
                raise BaseException("[AIndexQuotation] jqdata can not supply frequency: %d now" % frequency)
            data = AIndexQuotationJqData().get_quotation(securityIds=securityIds, items=items,
                                                         frequency=frequency, begin_datetime=begin_datetime,
                                                         end_datetime=end_datetime, adjusted=adjusted)
        else:
            raise BaseException("[AIndexQuotation] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data


class AIndexWeights:

    @staticmethod
    def get_index_weights(securityIds, date_datetime, dataSource=None):
        log = logger.get_logger(name="AIndexWeights")
        if not isinstance(securityIds, list):
            raise BaseException("[AIndexWeights] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AIndexWeights] securityId in securityIds must be str")
        if not isinstance(date_datetime, datetime.datetime):
            raise BaseException("[AIndexWeights] date_datetime must be datetime.datetime")
        if dataSource is None:
            dataSource = BarFeedConfig.get_client_config().get("AIndexWeights")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            data = AIndexWeightsWindDataBase().get_index_weights(securityIds=securityIds, date_datetime=date_datetime)
        else:
            raise BaseException("[AIndexWeights] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by=["indexId", "securityId"], axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data








