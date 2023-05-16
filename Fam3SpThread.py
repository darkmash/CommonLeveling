import os
import re
import math
from PyQt5.QtCore import pyqtSignal, QObject
import pandas as pd
import debugpy
from Leveling import Leveling
from Alarm import Alarm
from Cycling import Cycling


class Fam3SpThread(QObject):
    spReturnError = pyqtSignal(Exception)
    spReturnInfo = pyqtSignal(str)
    spReturnWarning = pyqtSignal(str)
    spReturnEnd = pyqtSignal(bool)
    spReturnPb = pyqtSignal(int)
    spReturnMaxPb = pyqtSignal(int)
    spReturnEmgLinkage = pyqtSignal(dict)
    spReturnEmgMscode = pyqtSignal(dict)

    def __init__(self, debugFlag, date, constDate, list_masterFile, moduleMaxCnt, nonModuleMaxCnt, emgHoldList, cb_round, df_etcOrderInput):
        super().__init__()
        self.isDebug = debugFlag
        self.date = date
        self.constDate = constDate
        self.list_masterFile = list_masterFile
        self.moduleMaxCnt = moduleMaxCnt
        self.nonModuleMaxCnt = nonModuleMaxCnt
        self.emgHoldList = emgHoldList
        self.cb_round = cb_round
        self.df_etcOrderInput = df_etcOrderInput

    def run(self):
        # pandas 경고없애기 옵션 적용
        pd.set_option('mode.chained_assignment', None)
        try:
            if self.isDebug:
                debugpy.debug_this_thread()
            maxPb = 200
            self.spReturnMaxPb.emit(maxPb)
            if self.moduleMaxCnt > 0:
                progress = 0
                self.spReturnPb.emit(progress)
                leveling = Leveling(self)
                df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave = leveling.loadLevelingList()
                df = leveling.preProcessing(df_leveling)
                if self.isDebug:
                    df.to_excel('.\\debug\\FAM3\\Sp\\flow1.xlsx')
                df = leveling.workDayCalculate(df)
                if self.isDebug:
                    df.to_excel('.\\debug\\FAM3\\Sp\\flow2.xlsx')
                df_levelingCompDate, df_ori = leveling.levelingByCompDate(df)
                if self.isDebug:
                    df_levelingCompDate.to_excel('.\\debug\\FAM3\\Sp\\flow3.xlsx')
                df_addSmtAssy, dict_smtCnt = leveling.addSmtAssyInfo(df_levelingCompDate)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\FAM3\\Sp\\flow4.xlsx')
                df_reflectSmtAssy, alarmNo, df_alarm = leveling.levelingBySmtAssy(df_addSmtAssy, dict_smtCnt)
                if self.isDebug:
                    df_reflectSmtAssy.to_excel('.\\debug\\FAM3\\Sp\\flow5.xlsx')
                df_levelingCapacity, alarmNo, df_alarm = leveling.levelingByCapacity(df_reflectSmtAssy, alarmNo, df_alarm)
                if self.isDebug:
                    df_levelingCapacity.to_excel('.\\debug\\FAM3\\Sp\\flow6.xlsx')
                    df_alarm.to_excel('.\\debug\\FAM3\\Sp\\alarm.xlsx')

                alarm = Alarm(self)
                alarm.summary(df_alarm)

                df_postProcessing, message = leveling.postProcessing(df_levelingCapacity, df_levelingCompDate)
                if self.isDebug:
                    df_postProcessing.to_excel('.\\debug\\FAM3\\Power\\flow7.xlsx')
                   
                cycling = Cycling(self)
                cycling.excute(df_postProcessing, df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave)
               
                self.spReturnEnd.emit(True)
                
            else:
                self.spReturnPb.emit(maxPb)
                self.spReturnEnd.emit(True)
                return
        except Exception as e:
            self.spReturnError.emit(e)
            return