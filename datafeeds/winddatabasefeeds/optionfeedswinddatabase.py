# -*- coding:utf-8 -*-
# @Time    : 2019-12-30 15:58
# @Author  : liuqiuxi
# @Email   : liuqiuxi1990@gmail.com
# @File    : optionfeedswinddatabase.py
# @Project : datafeeds
# @Software: PyCharm
# @Remark  : This is class of option market


from datafeeds.winddatabasefeeds import BaseWindDataBase


class AOptionQuotationWindDataBase(BaseWindDataBase):
    LOGGER_NAME = "AOptionQuotationWindDataBase"

    def __init__(self):
        super(AOptionQuotationWindDataBase, self).__init__()

    def get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime, adjusted=None):
        pass

    def __get_quotation(self, securityIds, items, frequency, begin_datetime, end_datetime):
        connect = self.connect()
        begin_datetime = begin_datetime.strftime("%Y%m%d")
        end_datetime = end_datetime.strftime("%Y%m%d")
        sqlClause = "SELECT owner FROM all_tables WHERE table_name = 'CHINAOPTIONEODPRICES'"
        owner = connect.execute(sqlClause).fetchone()
        if owner is None:
            raise BaseException("[%s] the CHINAOPTIONEODPRICES table do not exist" % self.LOGGER_NAME)
        else:
            owner = owner.values()[0] + "."
        if frequency != 86400:
            raise BaseException("[%s] we can't supply frequency: %d " % (self.LOGGER_NAME, frequency))
        if len(securityIds) == 0:
            sqlClause = ("select * from " + owner + "ChinaOptionEODPrices where trade_dt >= '" + begin_datetime + "' " +
                         "and trade_dt <= '" + end_datetime + "' and s_info_windcode = '" + securityIds[0] + "'")
            # sqlClause = ("select s_info_windcode as securityId, trade_dt as dateTime, s_dq_open as open, " +
            #              "s_dq_high as high, s_dq_low as low, s_dq_close as close, s_dq_settle as settle, " +
            #              "s_dq_volume as volume, s_dq_amount as amount, s_dq_oi as openInterest, s_dq_oichange " +
            #              "as openInterestChange, s_dq_presettle as preSettle, s_dq_change1 as cash_change, " +
            #              "s_dq_change1 as cash_change_settle_to_settle from " + owner + "ChinaOptionEODPrices " +
            #              "where trade_dt >= '" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' " +
            #              "and s_info_windcode = '" + securityIds[0] + "'")
        else:
            sqlClause = ("select * from " + owner + "ChinaOptionEODPrices where trade_dt >= '" + begin_datetime + "' " +
                         "and trade_dt <= '" + end_datetime + "' and s_info_windcode in " +
                         "" + str(tuple(securityIds)) + "")
            # sqlClause = ("select s_info_windcode as securityId, trade_dt as dateTime, s_dq_open as open, " +
            #              "s_dq_high as high, s_dq_low as low, s_dq_close as close, s_dq_settle as settle, " +
            #              "s_dq_volume as volume, s_dq_amount as amount, s_dq_oi as openInterest, s_dq_oichange " +
            #              "as openInterestChange, s_dq_presettle as preSettle, s_dq_change1 as cash_change, " +
            #              "s_dq_change1 as cash_change_settle_to_settle from " + owner + "ChinaOptionEODPrices " +
            #              "where trade_dt >= '" + begin_datetime + "' and trade_dt <= '" + end_datetime + "' " +
            #              "and s_info_windcode in " + str(tuple(securityIds)) + "")
        data = self.get_data_with_sql(sqlClause=sqlClause, connect=connect)

        pass


