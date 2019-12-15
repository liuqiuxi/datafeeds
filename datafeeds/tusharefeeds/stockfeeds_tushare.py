from datafeeds.tusharefeeds import BaseTuShare
import datetime
import pandas as pd


class AShareCalendarTuShare(BaseTuShare):

    def __init__(self):
        super(AShareCalendarTuShare, self).__init__()

    def get_calendar(self, begin_datetime, end_datetime):
        connect = self.connect_pro()
        begin_datetime = begin_datetime.strftime("%Y%m%d")
        end_datetime = end_datetime.strftime("%Y%m%d")
        data = connect.query(api_name="trade_cal", start_date=begin_datetime, end_date=end_datetime)
        data = data.loc[data.loc[:, "is_open"] == 1, :].copy(deep=True)
        data.drop_duplicates(subset=["cal_date"], keep="first", inplace=True)
        data.rename(columns={"cal_date": "dateTime"}, inplace=True)
        data.loc[:, "dateTime"] = data.loc[:, "dateTime"].apply(lambda x: datetime.datetime.strptime(x, "%Y%m%d"))
        data = pd.DataFrame(data={"dateTime": data.loc[:, "dateTime"]})
        data.sort_values(by="dateTime", axis=0, ascending=True, inplace=True)
        data.reset_index(drop=True, inplace=True)
        return data



