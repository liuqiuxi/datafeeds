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
            clientItems = clientItems + item + ","
        clientItems = clientItems[:-1]
        #get adjusted
        adjusted = self.__adjust_name_dict.get(adjusted, None)
        for securityId in securityIds:
            data0 = connect.wsd(codes=securityId, fields=clientItems, beginTime=begin_datetime, endTime=end_datetime,options=adjusted)
            data1 = pd.DataFrame({"securityId": securityId, "dateTime": data0.Times})
            for i in range(len(data0.Fields)):
                data1.loc[:, data0.Fields[i].lower()] = data0.Data[i]
            data = pd.concat(objs=[data, data1], axis=0, join="outer")
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
                
                
            
        
    
    