from datafeeds.tusharefeeds.stockfeeds_tushare import AShareCalendarTuShare

import datetime


class AShareCalendar:

    @staticmethod
    def get_calendar(begin_datetime, end_datetime, source="tuShare"):
        if not isinstance(begin_datetime, datetime.datetime):
            raise BaseException("[AShareCalendar] begin_datetime must be datetime type")
        if not isinstance(end_datetime, datetime.datetime):
            raise BaseException("[AShareCalendar] end_datetime must be datetime type")
        if source == "tuShare":
            data = AShareCalendarTuShare().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        else:
            raise BaseException("[AShareCalendar] the source: %s not support now" % source)
        data.loc[:, "dataSource"] = source
        return data


