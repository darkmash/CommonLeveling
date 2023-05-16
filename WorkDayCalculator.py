import pandas as pd
import numpy as np
import datetime


class WorkDayCalculator:
    def checkWorkDay(self, df, today, compDate):
        dtToday = pd.to_datetime(datetime.datetime.strptime(today, '%Y%m%d'))
        dtComp = pd.to_datetime(compDate, unit='s')
        workDay = 0
        if len(df.index[(df['WC_DATE'] == dtComp)].tolist()) > 0:
            index = int(df.index[(df['WC_DATE'] == dtComp)].tolist()[0])
            # Calculate workdays from current date to compDate
            while dtToday > pd.to_datetime(df['WC_DATE'][index], unit='s'):
                if df['WC_GUBUN'][index] == 1:
                    workDay -= 1
                index += 1
            # Calculate workdays from current date to compDate
            for i in df.index:
                dt = pd.to_datetime(df['WC_DATE'][i], unit='s')
                if dtToday < dt and dt <= dtComp:
                    if df['WC_GUBUN'][i] == 1:
                        workDay += 1
        else:
            # Emit warning if compDate not found in dataframe
            workDay = np.busday_count(begindates=dtToday.date(), enddates=dtComp.date())
        return workDay