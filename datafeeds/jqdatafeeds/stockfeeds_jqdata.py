from datafeeds.jqdatafeeds import BaseJQData
import pandas as pd
import datetime


class AShareCalendarJQData(BaseJQData):

    def __init__(self):
        super(AShareCalendarJQData, self).__init__()

    def get_calendar(self, begin_datetime, end_datetime):
        connect = self.connect()
        data = connect.get_trade_days(start_date=begin_datetime, end_date=end_datetime, count=None)
        data = pd.DataFrame(data={"dateTime": data})
        datetime_list = data.loc[:, "dateTime"].apply(lambda x: datetime.datetime.combine(date=x, time=datetime.time.min))
        data.loc[:, "dateTime"] = datetime_list
        data.drop_duplicates(subset=["dateTime"], keep="first", inplace=True)
        data.sort_values(by="dateTime", axis=0, ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        return data



