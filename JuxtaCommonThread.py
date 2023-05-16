from PyQt5.QtCore import pyqtSignal, QObject
import pandas as pd
import debugpy
from Leveling import Leveling
from Cycling import Cycling


class JuxtaCommonThread(QObject):
    # 클래스 외부에서 사용할 수 있도록 시그널 선언
    returnError = pyqtSignal(Exception)
    returnInfo = pyqtSignal(str)
    returnWarning = pyqtSignal(str)
    returnEnd = pyqtSignal(bool)
    returnDf = pyqtSignal(pd.DataFrame)
    returnPb = pyqtSignal(int)
    returnMaxPb = pyqtSignal(int)
    returnEmgLinkage = pyqtSignal(dict)
    returnEmgMscode = pyqtSignal(dict)

    def __init__(self, debugFlag, date, constDate, list_masterFile, firstOrdCnt, secondOrdCnt, emgHoldList):
        super().__init__(),
        self.isDebug = debugFlag
        self.date = date
        self.constDate = constDate
        self.list_masterFile = list_masterFile
        self.firstOrdCnt = firstOrdCnt
        self.secondOrdCnt = secondOrdCnt
        self.emgHoldList = emgHoldList

    def run(self):
        # pandas 경고없애기 옵션 적용
        pd.set_option('mode.chained_assignment', None)
        try:
            # 쓰레드 디버깅을 위한 처리
            if self.isDebug:
                debugpy.debug_this_thread()
            # 프로그레스바의 최대값 설정
            maxPb = 210
            self.returnMaxPb.emit(maxPb)
            if self.firstOrdCnt + self.secondOrdCnt > 0:
                progress = 0
                self.returnPb.emit(progress)
                leveling = Leveling(self)
                df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave = leveling.loadLevelingList()
                df = leveling.preProcessing(df_leveling)
                if self.isDebug:
                    df.to_excel('.\\debug\\JUXTA\\Old\\flow1.xlsx')
                df = leveling.workDayCalculate(df)
                if self.isDebug:
                    df.to_excel('.\\debug\\JUXTA\\Old\\flow2.xlsx')
                df_levelingJobDate = leveling.levelingByJobDate(df)
                if self.isDebug:
                    df_levelingJobDate.to_excel('.\\debug\\JUXTA\\Old\\flow3.xlsx')    

                df_levelingCompDate, df_ori = leveling.levelingByCompDate(df_levelingJobDate)
                if self.isDebug:
                    df_levelingCompDate.to_excel('.\\debug\\JUXTA\\Old\\flow4.xlsx')
                df_addSmtAssy, dict_smtCnt = leveling.addSmtAssyInfo(df_levelingCompDate)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\JUXTA\\Old\\flow5.xlsx')
                df_reflectSmtAssy, alarmNo, df_alarm = leveling.levelingBySmtAssy(df_addSmtAssy, dict_smtCnt)
                if self.isDebug:
                    df_reflectSmtAssy.to_excel('.\\debug\\JUXTA\\Old\\flow6.xlsx')
                    df_alarm.to_excel('.\\debug\\JUXTA\\Old\\alarm.xlsx')
                df_levelingCapacity, alarmNo, df_alarm = leveling.levelingByCapacity(df_reflectSmtAssy, alarmNo, df_alarm)
                if self.isDebug:
                    df_levelingCapacity.to_excel('.\\debug\\JUXTA\\Old\\flow7.xlsx')

                df_postProcessing = leveling.postProcessing(df_levelingCapacity, df_ori)
                if self.isDebug:
                    df_postProcessing.to_excel('.\\debug\\JUXTA\\Old\\flow8.xlsx')

                cycling = Cycling(self)
                cycling.excute(df_postProcessing, df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave)

        except Exception as e:
            self.returnError.emit(e)
            return