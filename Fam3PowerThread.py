import os
import math
from PyQt5.QtCore import pyqtSignal, QObject
import pandas as pd
import debugpy
from Leveling import Leveling
from Alarm import Alarm
from Cycling import Cycling

class Fam3PowerThread(QObject):
    powerReturnError = pyqtSignal(Exception)
    powerReturnInfo = pyqtSignal(str)
    powerReturnEnd = pyqtSignal(bool)
    powerReturnWarning = pyqtSignal(str)
    powerReturnPb = pyqtSignal(int)
    powerReturnMaxPb = pyqtSignal(int)
    powerReturnEmgLinkage = pyqtSignal(dict)
    powerReturnEmgMscode = pyqtSignal(dict)

    def __init__(self, debugFlag, date, constDate, list_masterFile, moduleMaxCnt, emgHoldList, cb_round, df_etcOrderInput):
        super().__init__()
        self.isDebug = debugFlag
        self.date = date
        self.constDate = constDate
        self.list_masterFile = list_masterFile
        self.moduleMaxCnt = moduleMaxCnt
        self.emgHoldList = emgHoldList
        self.cb_round = cb_round
        self.df_etcOrderInput = df_etcOrderInput

    def run(self):
        # pandas 경고없애기 옵션 적용
        pd.set_option('mode.chained_assignment', None)
        try:
            if self.isDebug:
                debugpy.debug_this_thread()
            alaramMaxCnt = self.moduleMaxCnt + int(self.df_etcOrderInput['착공량'][0])
            maxPb = 200
            self.powerReturnMaxPb.emit(maxPb)
            if self.moduleMaxCnt > 0:
                progress = 0
                self.powerReturnPb.emit(progress)
                leveling = Leveling(self)
                df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave = leveling.loadLevelingList()
                df = leveling.preProcessing(df_leveling)
                if self.isDebug:
                    df.to_excel('.\\debug\\FAM3\\Power\\flow1.xlsx')
                df = leveling.workDayCalculate(df)
                if self.isDebug:
                    df.to_excel('.\\debug\\FAM3\\Power\\flow2.xlsx')
                df_levelingCompDate, df_ori = leveling.levelingByCompDate(df)
                if self.isDebug:
                    df_levelingCompDate.to_excel('.\\debug\\FAM3\\Power\\flow3.xlsx')
                        
                df_addSmtAssy, dict_smtCnt = leveling.addSmtAssyInfo(df_levelingCompDate)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\FAM3\\Power\\flow4.xlsx')
                df_reflectSmtAssy, alarmNo, df_alarm = leveling.levelingBySmtAssy(df_addSmtAssy, dict_smtCnt)
                if self.isDebug:
                    df_reflectSmtAssy.to_excel('.\\debug\\FAM3\\Power\\flow5.xlsx')
                df_levelingCapacity, alarmNo, df_alarm = leveling.levelingByCapacity(df_reflectSmtAssy, alarmNo, df_alarm)
                if self.isDebug:
                    df_levelingCapacity.to_excel('.\\debug\\FAM3\\Power\\flow6.xlsx')
                    df_alarm.to_excel('.\\debug\\FAM3\\Main\\alarm.xlsx')

                alarm = Alarm(self)
                alarm.summary(df_alarm)

                df_postProcessing, message = leveling.postProcessing(df_levelingCapacity, df_levelingCompDate)
                if self.isDebug:
                    df_postProcessing.to_excel('.\\debug\\FAM3\\Power\\flow7.xlsx')
                   
                cycling = Cycling(self)
                cycling.excute(df_postProcessing, df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave)

            else:
                self.powerReturnPb.emit(maxPb)
            self.powerReturnEnd.emit(True)
            return
        except Exception as e:
            self.powerReturnError.emit(e)
            return
