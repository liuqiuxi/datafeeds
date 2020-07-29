# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 16:52:55 2020

@author: LIUQIUXI982
"""

from datafeeds.windclientfeeds import BaseWindClient
from datafeeds.utils import BarFeedConfig
from datafeeds import logger
import pandas as pd


class AFundQuotationWindClient(BaseWindClient):
    LOGGER_NAME = "AFundQuotationWindClient"
    
    def __init__(self):
        super(AFundQuotationWindClient, self).__init__()
        self.__adjust_name_dict = {"F": "PriceAdj=F", "B": "PriceAdj=B"}
        self.__items = {"preClose": "pre_close", "amount": "amt"}
        
        self.__logger = logger.get_logger(name=self.LOGGER_NAME)
        
    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
        connect = self.connect()
        begin_datetime = begin_datetime.strftime("%Y-%m-%d")
        end_datetime = end_datetime.strftime("%Y-%m-%d")
        if frequency != 86400:
            raise BaseException("[%s] we can't supply frequency: %d " % (self.LOGGER_NAME, frequency))
        data = pd.DataFrame()
        #format items
        clientItems = ""
        for item in items:
            if item in self.__items.keys():
                item = self.__items.get(item)
            clientItems = clientItems + item + ","
        clientItems = clientItems[:-1]
        #get adjusted
        adjusted = self.__adjust_name_dict.get(adjusted, None)
        for securityId in securityIds:
            data0 = connect.wsd(codes=securityId, fields=clientItems, beginTime=begin_datetime, endTime=end_datetime,options=adjusted)
            data1 = pd.DataFrame({"securityId": securityId, "dateTime": data0.Times})
            for i in range(len(data0.Fields)):
                data1.loc[:, data0.Fields[i].lower()] = data0.Data[i]
            data = data.append(data1)
        rename_dict = BarFeedConfig.get_windclient_items().get(self.LOGGER_NAME)
        data.rename(columns=rename_dict, inplace=True)
        log = logger.get_logger(name=self.LOGGER_NAME)
        default_items = list(rename_dict.values())
        real_items = []
        for item in items:
            if item in ["securityId", "dateTime"]:
                log.info("There is no need add item: %s to parameters items" % item)
            elif item in default_items:
                real_items.append(item)
            else:
                log.warning("item %s not in default items, so we remove this item to data" % item)
        data = data.loc[:, ["dateTime", "securityId"] + real_items].copy(deep=True)
        connect.stop()
        return data
    
    
class AFundIndustryWindClient(BaseWindClient):
    LOGGER_NAME = "AFundIndustryWindClient"

    def __init__(self):
        super(AFundIndustryWindClient, self).__init__()      
        
    def get_fund_track(self, securityIds, date_datetime):
        connect = self.connect()
        instruments = ""
        for securityId in securityIds:
            instruments = instruments + securityId + ","
        instruments = instruments[:-1]
        data = connect.wss(codes=instruments, fields="fund_trackindexcode")
        data = pd.DataFrame({"securityId": data.Codes, "industryName": data.Data[0]})
        data.loc[:, "dateTime"] = date_datetime
        connect.stop()
        return data
        
                
            
        
    
    