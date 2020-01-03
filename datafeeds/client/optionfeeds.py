# -*- coding:utf-8 -*-
# @Time    : 2019-12-30 19:29
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : optionfeeds.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is all data source option feeds

import datetime
from datafeeds import logger
from datafeeds.utils import BarFeedConfig
from datafeeds.winddatabasefeeds.optionfeedswinddatabase import AOptionQuotationWindDataBase
from datafeeds.jqdatafeeds.optionfeedsjqdata import AOptionQuotationJqData



class AOptionQuotation:

    @staticmethod
    def get_quotation(securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None, dataSource=None):
        if not isinstance(securityIds, list):
            raise BaseException("[AOptionQuotation] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AOptionQuotation] securityId in securityIds must be str")
        if not isinstance(items, list):
            raise BaseException("[AOptionQuotation] items must be list")
        for item in items:
            if not isinstance(item, str):
                raise BaseException("[AOptionQuotation] item in items must be str")
        if not isinstance(begin_datetime, datetime.datetime):
            raise BaseException("[AOptionQuotation] begin_datetime must be datetime.datetime")
        if not isinstance(end_datetime, datetime.datetime):
            raise BaseException("[AOptionQuotation] end_datetime must be datetime.datetime")
        if begin_datetime > end_datetime:
            raise BaseException("[AOptionQuotation] begin_datetime must less than end_datetime")
        if adjusted not in ["F", "B", None]:
            raise BaseException("[AOptionQuotation] adjusted only can in [F, B, None], not supply %s" % adjusted)
        if dataSource is None:
            log = logger.get_logger(name="AOptionQuotation")
            dataSource = BarFeedConfig.get_client_config().get("AOptionQuotation")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            if frequency != 86400:
                raise BaseException("[AIndexQuotation] wind can not supply frequency: %d now" % frequency)
            data = AOptionQuotationWindDataBase().get_quotation(securityIds=securityIds, items=items,
                                                                frequency=frequency, begin_datetime=begin_datetime,
                                                                end_datetime=end_datetime, adjusted=adjusted)
        elif dataSource == "jqdata":
            if frequency % 60 != 0:
                raise BaseException("[AOptionQuotation] jqdata can not supply frequency: %d now" % frequency)
            data = AOptionQuotationJqData().get_quotation(securityIds=securityIds, items=items,
                                                          frequency=frequency, begin_datetime=begin_datetime,
                                                          end_datetime=end_datetime, adjusted=adjusted)
        else:
            raise BaseException("[%s] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data