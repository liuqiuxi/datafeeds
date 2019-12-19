from datafeeds.jqdatafeeds.stockfeeds_jqdata import AShareCalendarJQData
from datafeeds.tusharefeeds.stockfeeds_tushare import AShareCalendarTuShare


class AShareCalendar:

    @staticmethod
    def get_calendar(begin_datetime, end_datetime, source="tuShare"):
        if source == "tuShare":
            data = AShareCalendarTuShare().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        elif source == "jqData":
            data = AShareCalendarJQData().get_calendar(begin_datetime=begin_datetime, end_datetime=end_datetime)
        else:
            raise BaseException("[AShareCalendar] not support %s data source" % source)
        data.loc[:, "dataSource"] = source
        return data


