# -*- coding:utf-8 -*-
# @Time    : 2019-12-31 11:27
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : fundfeeds.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is all data source fund feeds

import datetime
from datafeeds.utils import BarFeedConfig
from datafeeds import logger
from datafeeds.winddatabasefeeds.fundfeedswinddatabase import AFundQuotationWindDataBase
from datafeeds.jqdatafeeds.fundfeedsjqdata import AFundQuotationJqData
from datafeeds.jqdatafeeds.fundfeedsjqdata import AFundDayVarsJqData
from datafeeds.windclientfeeds.fundfeedswindclient import AFundIndustryWindClient


class AFundQuotation:

    @staticmethod
    def get_quotation(securityIds, items, frequency, begin_datetime, end_datetime, adjusted="B", dataSource=None):
        if not isinstance(securityIds, list):
            raise BaseException("[AFundQuotation] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AFundQuotation] securityId in securityIds must be str")
        if not isinstance(items, list):
            raise BaseException("[AFundQuotation] items must be list")
        for item in items:
            if not isinstance(item, str):
                raise BaseException("[AFundQuotation] item in items must be str")
        if not isinstance(begin_datetime, datetime.datetime):
            raise BaseException("[AFundQuotation] begin_datetime must be datetime.datetime")
        if not isinstance(end_datetime, datetime.datetime):
            raise BaseException("[AFundQuotation] end_datetime must be datetime.datetime")
        if begin_datetime > end_datetime:
            raise BaseException("[AFundQuotation] begin_datetime must less than end_datetime")
        if adjusted not in ["F", "B", None]:
            raise BaseException("[AFundQuotation] adjusted only can in [F, B, None], not supply %s" % adjusted)
        if dataSource is None:
            log = logger.get_logger(name="AFundQuotation")
            dataSource = BarFeedConfig.get_client_config().get("AFundQuotation")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            if frequency != 86400:
                raise BaseException("[AFundQuotation] wind can not supply frequency: %d now" % frequency)
            data = AFundQuotationWindDataBase().get_quotation(securityIds=securityIds, items=items,
                                                              frequency=frequency, begin_datetime=begin_datetime,
                                                              end_datetime=end_datetime, adjusted=adjusted)
        elif dataSource == "jqdata":
            if frequency % 60 != 0:
                raise BaseException("[AFutureQuotation] jqdata can not supply frequency: %d now" % frequency)
            data = AFundQuotationJqData().get_quotation(securityIds=securityIds, items=items, frequency=frequency,
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
    
    
class AFundDayVars:

    @staticmethod
    def get_value(date_datetime, dataSource=None):
        log = logger.get_logger(name="AFundDayVars")
        if not isinstance(date_datetime, datetime.datetime):
            raise BaseException("[AFundDayVars] date_datetime must be datetime.datetime")
        if dataSource is None:
            dataSource = BarFeedConfig.get_client_config().get("AFundDayVars")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "jqdata":
            data = AFundDayVarsJqData().get_value(date_datetime=date_datetime)
        else:
            raise BaseException("[AShareDayVars] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by="securityId", axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % date_datetime)
        return data
    
    
class AFundIndustry:
    
    @staticmethod
    def get_track_industry(securityIds, date_datetime, dataSource=None):
        log = logger.get_logger(name="AFundIndustry")
        if not isinstance(securityIds, list):
            raise BaseException("[AFundIndustry] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AFundIndustry] securityId in securityIds must be str")
        if not isinstance(date_datetime, datetime.datetime):
            raise BaseException("[AFundIndustry] date_datetime must be datetime.datetime")
        if dataSource is None:
            dataSource = BarFeedConfig.get_client_config().get("AFundIndustry")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "windclient":
            data = AFundIndustryWindClient().get_wind_industry(securityIds=securityIds, date_datetime=date_datetime)
        else:
            raise BaseException("[AFundIndustry] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by="securityId", axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data
    
    
    
    
    
    
    
    
    
