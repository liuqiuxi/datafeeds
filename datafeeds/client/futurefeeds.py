# -*- coding:utf-8 -*-
# @Time    : 2019-12-30 15:31
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : futurefeeds.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is all data source future feeds

import datetime
from datafeeds import logger
from datafeeds.utils import BarFeedConfig
from datafeeds.winddatabasefeeds.futurefeedswinddatabase import AFutureQuotationWindDataBase
from datafeeds.jqdatafeeds.futurefeedsjqdata import AFutureQuotationJqData
from datafeeds.tusharefeeds.futurefeedstushare import AFutureQuotationTuShare


class AFutureQuotation:

    @staticmethod
    def get_quotation(securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None, dataSource=None):
        if not isinstance(securityIds, list):
            raise BaseException("[AFutureQuotation] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AFutureQuotation] securityId in securityIds must be str")
        if not isinstance(items, list):
            raise BaseException("[AFutureQuotation] items must be list")
        for item in items:
            if not isinstance(item, str):
                raise BaseException("[AFutureQuotation] item in items must be str")
        if not isinstance(begin_datetime, datetime.datetime):
            raise BaseException("[AFutureQuotation] begin_datetime must be datetime.datetime")
        if not isinstance(end_datetime, datetime.datetime):
            raise BaseException("[AFutureQuotation] end_datetime must be datetime.datetime")
        if begin_datetime > end_datetime:
            raise BaseException("[AFutureQuotation] begin_datetime must less than end_datetime")
        if adjusted not in ["F", "B", None]:
            raise BaseException("[AFutureQuotation] adjusted only can in [F, B, None], not supply %s" % adjusted)
        if dataSource is None:
            log = logger.get_logger(name="AFutureQuotation")
            dataSource = BarFeedConfig.get_client_config().get("AFutureQuotation")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            if frequency != 86400:
                raise BaseException("[AFutureQuotation] wind can not supply frequency: %d now" % frequency)
            data = AFutureQuotationWindDataBase().get_quotation(securityIds=securityIds, items=items,
                                                                frequency=frequency, begin_datetime=begin_datetime,
                                                                end_datetime=end_datetime, adjusted=adjusted)
        elif dataSource == "jqdata":
            if frequency % 60 != 0:
                raise BaseException("[AFutureQuotation] jqdata can not supply frequency: %d now" % frequency)
            data = AFutureQuotationJqData().get_quotation(securityIds=securityIds, items=items,
                                                          frequency=frequency, begin_datetime=begin_datetime,
                                                          end_datetime=end_datetime, adjusted=adjusted)
        elif dataSource == "tushare":
            data = AFutureQuotationTuShare().get_quotation(securityIds=securityIds, items=items, frequency=frequency,
                                                           begin_datetime=begin_datetime, end_datetime=end_datetime,
                                                           adjusted=adjusted)
        else:
            raise BaseException("[%s] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data