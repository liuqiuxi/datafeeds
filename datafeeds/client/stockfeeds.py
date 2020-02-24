# -*- coding:utf-8 -*-
# @Time    : 2019-12-27 16:45
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : stockfeeds.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is all data source stock feeds

import datetime
from datafeeds.utils import BarFeedConfig
from datafeeds import logger
from datafeeds.winddatabasefeeds.stockfeedswinddatabase import AShareCalendarWindDataBase
from datafeeds.tusharefeeds.stockfeedstushare import AShareCalendarTuShare
from datafeeds.jqdatafeeds.stockfeedsjqdata import AShareCalendarJqData
from datafeeds.windclientfeeds.stockfeedswindclient import AShareCalendarWindClient
from datafeeds.winddatabasefeeds.stockfeedswinddatabase import AShareQuotationWindDataBase
from datafeeds.jqdatafeeds.stockfeedsjqdata import AShareQuotationJqData
from datafeeds.tusharefeeds.stockfeedstushare import AShareQuotationTuShare
from datafeeds.windclientfeeds.stockfeedswindclient import AShareIndustryWindClient
from datafeeds.winddatabasefeeds.stockfeedswinddatabase import AShareIPOWindDataBase
from datafeeds.winddatabasefeeds.stockfeedswinddatabase import AShareDayVarsWindDataBase
from datafeeds.jqdatafeeds.stockfeedsjqdata import AShareDayVarsJqData
from datafeeds.jqdatafeeds.stockfeedsjqdata import AShareIndustryJqData


class AShareCalendar:

    @staticmethod
    def get_calendar(begin_datetime, end_datetime, dataSource=None):
        if not isinstance(begin_datetime, datetime.datetime):
            raise BaseException("[AShareCalendar] begin_datetime must be datetime.datetime")
        if not isinstance(end_datetime, datetime.datetime):
            raise BaseException("[AShareCalendar] end_datetime must be datetime.datetime")
        if begin_datetime > end_datetime:
            raise BaseException("[AShareCalendar] begin_datetime must less than end_datetime")
        if dataSource is None:
            log = logger.get_logger(name="AShareCalendar")
            dataSource = BarFeedConfig.get_client_config().get("AShareCalendar")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            data = AShareCalendarWindDataBase().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        elif dataSource == "tushare":
            data = AShareCalendarTuShare().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        elif dataSource == "jqdata":
            data = AShareCalendarJqData().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        elif dataSource == "windclient":
            data = AShareCalendarWindClient().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        else:
            raise BaseException("[AShareCalendar] dataSource: %s can't supply now" % dataSource)
        data.loc[:, "dateSource"] = dataSource
        data.drop_duplicates(subset=["dateTime"], inplace=True)
        data.sort_values(by="dateTime", axis=0, ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        return data


class AShareQuotation:

    @staticmethod
    def get_quotation(securityIds, items, frequency, begin_datetime, end_datetime, adjusted="B", dataSource=None):
        log = logger.get_logger(name="AShareQuotation")
        if not isinstance(securityIds, list):
            raise BaseException("[AShareQuotation] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AShareQuotation] securityId in securityIds must be str")
        if not isinstance(items, list):
            raise BaseException("[AShareQuotation] items must be list")
        for item in items:
            if not isinstance(item, str):
                raise BaseException("[AShareQuotation] item in items must be str")
        if not isinstance(begin_datetime, datetime.datetime):
            raise BaseException("[AShareQuotation] begin_datetime must be datetime.datetime")
        if not isinstance(end_datetime, datetime.datetime):
            raise BaseException("[AShareQuotation] end_datetime must be datetime.datetime")
        if begin_datetime > end_datetime:
            raise BaseException("[AShareQuotation] begin_datetime must less than end_datetime")
        if adjusted not in ["F", "B", None]:
            raise BaseException("[AShareQuotation] adjusted only can in [F, B, None], not supply %s" % adjusted)
        if dataSource is None:
            dataSource = BarFeedConfig.get_client_config().get("AShareQuotation")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            if frequency != 86400:
                raise BaseException("[AShareQuotation] wind can not supply frequency: %d now" % frequency)
            data = AShareQuotationWindDataBase().get_quotation(securityIds=securityIds, items=items,
                                                               frequency=frequency, begin_datetime=begin_datetime,
                                                               end_datetime=end_datetime, adjusted=adjusted)
        elif dataSource == "jqdata":
            if frequency % 60 != 0:
                raise BaseException("[AFutureQuotation] jqdata can not supply frequency: %d now" % frequency)
            data = AShareQuotationJqData().get_quotation(securityIds=securityIds, items=items,
                                                         frequency=frequency, begin_datetime=begin_datetime,
                                                         end_datetime=end_datetime, adjusted=adjusted)
        elif dataSource == "tushare":
            data = AShareQuotationTuShare().get_quotation(securityIds=securityIds, items=items,
                                                         frequency=frequency, begin_datetime=begin_datetime,
                                                         end_datetime=end_datetime, adjusted=adjusted)
        else:
            raise BaseException("[AShareQuotation] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by=["securityId", "dateTime"], axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data


class AShareIndustry:

    @staticmethod
    def get_sw_industry(securityIds, date_datetime, lv=1, dataSource=None):
        log = logger.get_logger(name="AShareIndustry")
        if not isinstance(securityIds, list):
            raise BaseException("[AShareIndustry] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AShareQuotation] securityId in securityIds must be str")
        if not isinstance(date_datetime, datetime.datetime):
            raise BaseException("[AShareIndustry] date_datetime must be datetime.datetime")
        if not isinstance(lv, int):
            raise BaseException("[AShareIndustry] lv type must be int")
        if lv not in [1, 2, 3]:
            raise BaseException("[AShareIndustry] lv only can in [1, 2, 3], lv: %d not support now" % lv)
        if dataSource is None:
            dataSource = BarFeedConfig.get_client_config().get("AShareIndustry")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "windclient":
            data = AShareIndustryWindClient().get_sw_industry(securityIds=securityIds,
                                                              date_datetime=date_datetime,
                                                              lv=lv)
        elif dataSource == "jqdata":
            data = AShareIndustryJqData().get_sw_industry(securityIds=securityIds,
                                                          ate_datetime=date_datetime,
                                                          lv=lv)
        else:
            raise BaseException("[AShareIndustry] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by="securityId", axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data

    @staticmethod
    def get_zx_industry(securityIds, date_datetime, lv=1, dataSource=None):
        log = logger.get_logger(name="AShareIndustry")
        if not isinstance(securityIds, list):
            raise BaseException("[AShareIndustry] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AShareQuotation] securityId in securityIds must be str")
        if not isinstance(date_datetime, datetime.datetime):
            raise BaseException("[AShareIndustry] date_datetime must be datetime.datetime")
        if not isinstance(lv, int):
            raise BaseException("[AShareIndustry] lv type must be int")
        if lv not in [1, 2, 3]:
            raise BaseException("[AShareIndustry] lv only can in [1, 2, 3], lv: %d not support now" % lv)
        if dataSource is None:
            dataSource = BarFeedConfig.get_client_config().get("AShareIndustry")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "windclient":
            data = AShareIndustryWindClient().get_zx_industry(securityIds=securityIds,
                                                              date_datetime=date_datetime,
                                                              lv=lv)
        else:
            raise BaseException("[AShareIndustry] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by="securityId", axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data

    @staticmethod
    def get_wind_industry(securityIds, date_datetime, lv=1, dataSource=None):
        log = logger.get_logger(name="AShareIndustry")
        if not isinstance(securityIds, list):
            raise BaseException("[AShareIndustry] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AShareQuotation] securityId in securityIds must be str")
        if not isinstance(date_datetime, datetime.datetime):
            raise BaseException("[AShareIndustry] date_datetime must be datetime.datetime")
        if not isinstance(lv, int):
            raise BaseException("[AShareIndustry] lv type must be int")
        if lv not in [1, 2, 3]:
            raise BaseException("[AShareIndustry] lv only can in [1, 2, 3], lv: %d not support now" % lv)
        if dataSource is None:
            dataSource = BarFeedConfig.get_client_config().get("AShareIndustry")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "windclient":
            data = AShareIndustryWindClient().get_wind_industry(securityIds=securityIds,
                                                                date_datetime=date_datetime,
                                                                lv=lv)
        else:
            raise BaseException("[AShareIndustry] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by="securityId", axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data


class AShareIPO:

    @staticmethod
    def get_initial_public_offering(securityIds, dataSource=None):
        log = logger.get_logger(name="AShareIPO")
        if not isinstance(securityIds, list):
            raise BaseException("[AShareIPO] securityIds must be list")
        for securityId in securityIds:
            if not isinstance(securityId, str):
                raise BaseException("[AShareIPO] securityId in securityIds must be str")
        if dataSource is None:
            dataSource = BarFeedConfig.get_client_config().get("AShareIPO")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            data = AShareIPOWindDataBase().get_initial_public_offering(securityIds=securityIds)
        else:
            raise BaseException("[AShareIPO] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by="securityId", axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data


class AShareDayVars:

    @staticmethod
    def get_value(date_datetime, dataSource=None):
        log = logger.get_logger(name="AShareDayVars")
        if not isinstance(date_datetime, datetime.datetime):
            raise BaseException("[AShareDayVars] date_datetime must be datetime.datetime")
        if dataSource is None:
            dataSource = BarFeedConfig.get_client_config().get("AShareDayVars")
            log.info("dataSource did't allocated, so we use init config: %s" % dataSource)
        if dataSource == "wind":
            data = AShareDayVarsWindDataBase().get_value(date_datetime=date_datetime)
        elif dataSource == "jqdata":
            data = AShareDayVarsJqData().get_value(date_datetime=date_datetime)
        else:
            raise BaseException("[AShareDayVars] dataSource: %s can't supply now" % dataSource)
        if not data.empty:
            data.sort_values(by="securityId", axis=0, ascending=True, inplace=True)
            data.reset_index(drop=True, inplace=True)
            data.loc[:, "dateSource"] = dataSource
        else:
            log.warning("%s did't get data, please check it " % securityIds)
        return data
























