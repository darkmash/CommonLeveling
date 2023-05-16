import os
from PyQt5.QtCore import pyqtSignal, QObject
import pandas as pd
import debugpy
import time
from Leveling import Leveling
from Alarm import Alarm
from Cycling import Cycling


class Fam3MainThread(QObject):
    # 클래스 외부에서 사용할 수 있도록 시그널 선언
    mainReturnError = pyqtSignal(Exception)
    mainReturnInfo = pyqtSignal(str)
    mainReturnWarning = pyqtSignal(str)
    mainReturnEnd = pyqtSignal(bool)
    mainReturnDf = pyqtSignal(pd.DataFrame)
    mainReturnPb = pyqtSignal(int)
    mainReturnMaxPb = pyqtSignal(int)
    mainReturnEmgLinkage = pyqtSignal(dict)
    mainReturnEmgMscode = pyqtSignal(dict)

    # 초기화
    def __init__(self, debugFlag, date, constDate, list_masterFile, moduleMaxCnt, emgHoldList, cb_round, df_etcOrderInput):
        super().__init__(),
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
            # 쓰레드 디버깅을 위한 처리
            if self.isDebug:
                debugpy.debug_this_thread()
            # 프로그레스바의 최대값 설정
            maxPb = 210
            self.mainReturnMaxPb.emit(maxPb)
            # 최대착공량 0 초과 입력할 경우에만 로직 실행
            if self.moduleMaxCnt > 0:
                progress = 0
                self.mainReturnPb.emit(progress)
                leveling = Leveling(self)
                df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave = leveling.loadLevelingList()
                df = leveling.preProcessing(df_leveling)
                if self.isDebug:
                    df.to_excel('.\\debug\\FAM3\\Main\\flow1.xlsx')
                df = leveling.workDayCalculate(df)
                if self.isDebug:
                    df.to_excel('.\\debug\\FAM3\\Main\\flow2.xlsx')
                df_levelingCompDate, df_ori = leveling.levelingByCompDate(df)
                if self.isDebug:
                    df_levelingCompDate.to_excel('.\\debug\\FAM3\\Main\\flow3.xlsx')
                        
                df_addSmtAssy, dict_smtCnt = leveling.addSmtAssyInfo(df_levelingCompDate)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\FAM3\\Main\\flow4.xlsx')
                df_reflectSmtAssy, alarmNo, df_alarm = leveling.levelingBySmtAssy(df_addSmtAssy, dict_smtCnt)
                if self.isDebug:
                    df_reflectSmtAssy.to_excel('.\\debug\\FAM3\\Main\\flow5.xlsx')
                df_levelingCapacity, alarmNo, df_alarm = leveling.levelingByCapacity(df_reflectSmtAssy, alarmNo, df_alarm)
                if self.isDebug:
                    df_levelingCapacity.to_excel('.\\debug\\FAM3\\Main\\flow6.xlsx')
                    df_alarm.to_excel('.\\debug\\FAM3\\Main\\alarm.xlsx')

                alarm = Alarm(self)
                alarm.summary(df_alarm)

                df_postProcessing, message = leveling.postProcessing(df_levelingCapacity, df_ori)
                if self.isDebug:
                    df_postProcessing.to_excel('.\\debug\\FAM3\\Main\\flow7.xlsx')
                   
                cycling = Cycling(self)
                cycling.excute(df_postProcessing, df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave)

            else:
                df_returnSp = pd.DataFrame()
                self.mainReturnDf.emit(df_returnSp)
                self.mainReturnPb.emit(maxPb)
            self.mainReturnEnd.emit(True)
            return
        except Exception as e:
            self.mainReturnError.emit(e)
            return
