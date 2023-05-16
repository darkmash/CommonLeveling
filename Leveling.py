import pandas as pd
import numpy as np
from pathlib import Path
import datetime
import math
import os
import re
import time
from configparser import ConfigParser
from WorkDayCalculator import WorkDayCalculator
from OracleDBReader import OracleDBReader
from Alarm import Alarm


class Leveling():
    def __init__(self, productClass=None):
        super().__init__()
        self.isDebug = productClass.isDebug             # Debug mode flag
        self.className = type(productClass).__name__    # Class name of the product
        # Set the initial module count based on the product class
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SPThread':
            self.firstModCnt = productClass.moduleMaxCnt
        elif self.className == 'JuxtaOldThread':
            self.firstModCnt = productClass.firstOrdCnt
        self.emgHoldList = productClass.emgHoldList             # Emergency hold list
        self.list_masterFile = productClass.list_masterFile     # List of master files
        self.constDate = productClass.constDate                 # Constant date value
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SPThread':
            self.cb_round = productClass.cb_round               # Round callback function
        else:
            self.cb_round = ''
        self.date = productClass.date                           # Date of order
        self.productClass = productClass                        # Reference to the product class
        self.parser = self.initParser()                         # Initialize the parser
        self.neuronDb = self.loadNeuronDB()                     # Load the Neuron database
        # Set the second module count based on the product class
        if self.className == 'Fam3SpThread':
            self.secondModCnt = productClass.nonModuleMaxCnt
        elif self.className == 'JuxtaOldThread':
            self.secondModCnt = productClass.secondOrdCnt
        else:
            self.secondModCnt = 0

    def initParser(self):
        parser = ConfigParser()  # Create a ConfigParser object
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SPThread':
            # If the class name is one of the Fam3 classes, read the master file at index 16 with euc-kr encoding
            parser.read(self.list_masterFile[16], encoding='euc-kr')
        elif self.className == 'JuxtaOldThread':
            # If the class name is JuxtaOldThread, read the master file at index 5 with euc-kr encoding
            parser.read(self.list_masterFile[5], encoding='euc-kr')
        return parser  # Return the parser object

    # 하이픈 삭제
    def _delHypen(self, value):
        return str(value).split('-')[0]
    
    # 생산시간 합계용 내부함수
    def getSec(self, time_str):
        """
        HH:MM:SS 형식의 시간 문자열을 초로 변환

        Args:
        - time_str: HH:MM:SS 형식의 시간 값을 나타내는 문자열

        Returns:
        - 시간 값을 초 단위로 나타내는 정수
        """
        time_str = re.sub(r'[^0-9:]', '', str(time_str))  # Remove non-numeric and non-colon characters from the time string
        if len(time_str) > 0:
            h, m, s = time_str.split(':')  # Split the time string into hours, minutes, and seconds
            return int(h) * 3600 + int(m) * 60 + int(s)  # Convert the time values to seconds and return the total sum
        else:
            return 0  # If the time string is empty, return 0 seconds

    # 백슬래시 삭제용 내부함수
    def delBackslash(self, value):
        """
        문자열에서 백슬래시를 제거하는 함수

        Args:
        - value: 백슬래시를 제거할 문자열

        Returns:
        - 백슬래시가 제거된 문자열
        """
        value = re.sub(r"\\c", "", str(value))  # Remove backslashes from the string
        return value  # Return the string with backslashes removed

    def loadNeuronDB(self):
        """
        NEURON DB를 로드하는 함수

        Returns:
        - 로드된 NEURON DB 객체
        """
        # NEURON DB 정보 가져오기
        neuronDbHost = self.parser.get('NEURON DB정보', 'Host')
        neuronDbPort = self.parser.getint('NEURON DB정보', 'Port')
        neuronDbSID = self.parser.get('NEURON DB정보', 'SID')
        neuronDbUser = self.parser.get('NEURON DB정보', 'Username')
        neuronDbPw = self.parser.get('NEURON DB정보', 'Password')
        
        # OracleDBReader를 사용하여 NEURON DB에 연결
        neuronDb = OracleDBReader(neuronDbHost, neuronDbPort, neuronDbSID, neuronDbUser, neuronDbPw)
        return neuronDb

    def loadFam3ProductDB(self):
        """
        FAM3 공수 계산 DB를 로드하는 함수

        Returns:
        - 로드된 FAM3 공수 계산 DB 객체
        """
        # FAM3 공수 계산 DB 정보 가져오기
        fam3ProductDbHost = self.parser.get('FAM3공수계산DB 정보', 'Host')
        fam3ProductDbPort = self.parser.getint('FAM3공수계산DB 정보', 'Port')
        fam3ProductDbSID = self.parser.get('FAM3공수계산DB 정보', 'SID')
        fam3ProductDbUser = self.parser.get('FAM3공수계산DB 정보', 'Username')
        fam3ProductDbPw = self.parser.get('FAM3공수계산DB 정보', 'Password')
        
        # OracleDBReader를 사용하여 FAM3 공수 계산 DB에 연결
        fam3ProductDb = OracleDBReader(fam3ProductDbHost, fam3ProductDbPort, fam3ProductDbSID, fam3ProductDbUser, fam3ProductDbPw)
        return fam3ProductDb

    def loaddfAteP(self):
        df_spCondition = pd.read_excel(self.list_masterFile[7])
        df_ateP = df_spCondition[df_spCondition['검사호기'] == 'P']
        df_ateP['1차_MAX_그룹'] = df_ateP['1차_MAX_그룹'].fillna(method='ffill')
        df_ateP['2차_MAX_그룹'] = df_ateP['2차_MAX_그룹'].fillna(method='ffill')
        df_ateP['1차_MAX'] = df_ateP['1차_MAX'].fillna(method='ffill')
        df_ateP['2차_MAX'] = df_ateP['2차_MAX'].fillna(method='ffill')
        df_ateP['우선착공'] = df_ateP['우선착공'].fillna(method='ffill')
        df_ateP['특수대상'] = '대상'
        return df_ateP

    def convertSecToTime(self, seconds):
        """
        초 단위의 시간을 시/분/초로 분할하여 문자열로 반환하는 함수

        Args:
        - seconds: 초 단위의 시간을 나타내는 정수 값

        Returns:
        - 시/분/초 형식의 문자열
        """
        # 총 초수를 24시간 단위로 나누어 하루를 초과하는 부분을 제거
        seconds = seconds % (24 * 3600)
        
        # 시간 계산
        hour = seconds // 3600
        seconds %= 3600
        
        # 분 계산
        minutes = seconds // 60
        seconds %= 60
        
        # 시/분/초 형식의 문자열로 변환하여 반환
        return "%d:%02d:%02d" % (hour, minutes, seconds)

    def workDayCalculate(self, df):
        """
        워킹데이 계산 함수

        Args:
        - df: 워킹데이를 계산할 데이터프레임

        Returns:
        - 워킹데이가 계산된 데이터프레임
        """
        # 워킹데이 캘린더 불러오기
        today = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            today = self.date
        df_calendar = self.neuronDb.readDB("SELECT WC_DATE, WC_GUBUN FROM WK_CALENDAR WHERE WC_DATE >= (SYSDATE - 365) ORDER BY WC_DATE ASC")
        df_calendar['WC_GUBUN'] = df_calendar['WC_GUBUN'].astype(np.int64)
        workDayCalculator = WorkDayCalculator()

        # 남은 워킹데이 체크 및 컬럼 추가
        for i in df.index:
            df['남은 워킹데이'][i] = workDayCalculator.checkWorkDay(df_calendar, today, df['Planned Prod. Completion date'][i])
            if df['남은 워킹데이'][i] < 1:
                df['긴급오더'][i] = '대상'
            elif df['남은 워킹데이'][i] == 1:
                df['당일착공'][i] = '대상'

        df['Linkage Number'] = df['Linkage Number'].astype(str)

        # 홀딩오더는 제외
        df = df[df['홀딩오더'].isnull()]
        return df

    # SMT Assy 반영 착공로직
    def smtReflectInst(self, df_input, isRemain, dict_smtCnt, alarmNo, df_alarm, rowNo):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_smtCnt(Dict)           : Smt잔여량 Dict
            alarmNo(int)          : 알람 번호
            df_alarm(DataFrame)   : 알람 상세 기록용 DataFrame
            rowNo(int)                  : 사용 Smt Assy 갯수
        Return:
            return(List)
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_smtCnt(Dict)           : Smt잔여량 Dict (갱신 후)
                alarmNo(int)          : 알람 번호
                df_alarm(DataFrame)   : 알람 상세 기록용 DataFrame (갱신 후)
        """
        instCol = '평준화_적용_착공량'
        resultCol = 'SMT반영_착공량'
        if isRemain:
            instCol = '잔여_착공량'
            resultCol = 'SMT반영_착공량_잔여'
        # 행별로 확인
        for i in df_input.index:
            # BU는 SMT Assy를 확인하지 않음.
            if self.className == 'Fam3PowerThread' and df_input['MS Code'][i][:4] == 'F3BU':
                df_input[resultCol][i] = df_input[instCol][i]
            else:
                rowCnt = self._getRowCnt(df_input, i, rowNo)
                minCnt = 9999
                isManageSMT = self._getIsManageSMT(df_input, i, rowCnt)
                minCnt, df_alarm = self._getMinCnt(df_input, i, instCol, dict_smtCnt, isManageSMT, isRemain, rowCnt, alarmNo, df_alarm)
                if minCnt != 9999:
                    df_input[resultCol][i] = minCnt
                else:
                    df_input[resultCol][i] = df_input[instCol][i]
                smtAssyName = ''
                for j in range(1, rowCnt + 1):
                    smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                    self._updateSmtCnt(df_input, i, smtAssyName, dict_smtCnt, resultCol)
        df_alarm = df_alarm.drop_duplicates(['L/N', '분류'])
        df_alarm['No.'] = df_alarm.index.astype(int) + 1
        df_alarm = df_alarm.reset_index(drop=True)
        return [df_input, dict_smtCnt, alarmNo, df_alarm]

    def _getRowCnt(self, df_input, i, rowNo):
        rowCnt = 1
        for j in range(1, rowNo):
            if str(df_input[f'ROW{str(j)}'][i]) not in ['', 'nan', 'None']:
                rowCnt = j
            else:
                break
        if rowNo == 1:
            rowCnt = 1
        return rowCnt

    def _getIsManageSMT(self, df_input, i, rowCnt):
        isManageSMT = True
        for j in range(1, rowCnt + 1):
            if self.className == 'Fam3SpThread' and str(df_input[f'SMT비관리대상{str(j)}'][i]) == 'True':
                isManageSMT = False
            elif self.className == 'JuxtaOldThread' and str(df_input[f'SMT비관리대상{str(j)}'][i]) == 'True':
                isManageSMT = False
        return isManageSMT

    def _getMinCnt(self, df_input, i, instCol, dict_smtCnt, isManageSMT, isRemain, rowCnt, alarmNo, df_alarm):
        alarm = Alarm(self.productClass)
        smtAssyName = ''
        minCnt = 9999
        for j in range(1, rowCnt + 1):
            smtAssyName = str(df_input[f'ROW{str(j)}'][i])
            if df_input['SMT_MS_CODE'][i] not in ['', 'nan', 'None'] and smtAssyName not in ['', 'nan', 'None']:
                if isManageSMT:
                    if df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상' and not isRemain:
                        if smtAssyName in dict_smtCnt:
                            if dict_smtCnt[smtAssyName] < 0:
                                diffCnt = df_input['미착공수주잔'][i]
                                if dict_smtCnt[smtAssyName] + df_input['미착공수주잔'][i] > 0:
                                    diffCnt = 0 - dict_smtCnt[smtAssyName]
                                if not isRemain and dict_smtCnt[smtAssyName] > 0:
                                    df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '1', df_input, i, smtAssyName, diffCnt)
                        else:
                            minCnt = 0
                            df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타3', df_input, i, smtAssyName, 0)
                    else:
                        if smtAssyName in dict_smtCnt:
                            if dict_smtCnt[smtAssyName] >= df_input[instCol][i]:
                                if minCnt > df_input[instCol][i]:
                                    minCnt = df_input[instCol][i]
                            else:
                                if dict_smtCnt[smtAssyName] > 0:
                                    if minCnt > dict_smtCnt[smtAssyName]:
                                        minCnt = dict_smtCnt[smtAssyName]
                                else:
                                    minCnt = 0
                                if not isRemain and dict_smtCnt[smtAssyName] > 0:
                                    df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '1', df_input, i, smtAssyName, df_input[instCol][i] - dict_smtCnt[smtAssyName])
                        else:
                            minCnt = 0
                            df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타3', df_input, i, smtAssyName, 0)
                else:
                    minCnt = df_input[instCol][i]
                    break
            else:
                if isManageSMT:
                    minCnt = 0
                    df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타1', df_input, i, '미등록', 0)
                else:
                    minCnt = df_input[instCol][i]
                    df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타1', df_input, i, '미등록', 0)
                    break   
        return minCnt, df_alarm

    def _updateSmtCnt(self, df_input, i, smtAssyName, dict_smtCnt, resultCol):
        if smtAssyName not in ['', 'nan', 'None']:
            if smtAssyName in dict_smtCnt:
                dict_smtCnt[smtAssyName] -= df_input[resultCol][i]
            else:
                dict_smtCnt[smtAssyName] = 0 - df_input[resultCol][i]
        return dict_smtCnt

    # 검사설비 반영 착공로직
    def fam3MainCapaReflect(self, df_input, isRemain, dict_ate, df_alarm, alarmNo, modCnt, limitCtCnt):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_ate(Dict)              : 잔여 검사설비능력 Dict
            alarmNo(int)                : 알람 번호
            df_alarm(DataFrame)         : 알람 상세 기록용 DataFrame
            modCnt(int)                 : 최대착공량
            limitCtCnt(int)             : CT제한 착공량
        Return:
            return(List)
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_ate(Dict)              : 잔여 검사설비능력 Dict (갱신 후)
                alarmNo(int)                : 알람 번호
                df_alarm(DataFrame)         : 알람 상세 기록용 DataFrame (갱신 후)
                modCnt(int)                 : 최대착공량 (갱신 후)
                limitCtCnt(int)             : CT제한 착공량 (갱신 후)
        """
        alarm = Alarm(self.productClass)
        # 여유 착공량인지 확인 후, 컬럼명을 지정
        smtReflectCnt = 'SMT반영_착공량_잔여' if isRemain else 'SMT반영_착공량'
        tempAteCnt = '임시수량_잔여' if isRemain else '임시수량'
        ateReflectCnt = '설비능력반영_착공량_잔여' if isRemain else '설비능력반영_착공량'
        for i in df_input.index:
            # 디버그로 남은착공량의 과정을 보기 위해 데이터프레임에 기록
            df_input['남은착공량'][i] = modCnt
            # 검사시간이 있는 모델만 적용
            if str(df_input['TotalTime'][i]) not in ['', 'nan']:
                # 검사설비가 있는 모델만 적용
                if str(df_input['INSPECTION_EQUIPMENT'][i]) not in ['', 'nan']:
                    # 임시 검사시간과 검사설비를 가지고 있는 변수 선언
                    tempTime = 0
                    ateName = ''
                    # 긴급오더 or 당일착공 대상은 검사설비 능력이 부족하여도 강제 착공. 그리고 알람을 기록
                    if (str(df_input['긴급오더'][i]) == '대상') or (str(df_input['당일착공'][i]) == '대상'):
                        # 대상 검사설비를 한개씩 분할
                        list_ate = list(df_input['INSPECTION_EQUIPMENT'][i])
                        # 입력되어진 검사설비 시간을 임시 검사설비 딕셔너리에 넣음.
                        dict_temp = {ate: dict_ate[ate] for ate in list_ate}
                        # 여유있는 검사설비 순으로 정렬
                        dict_temp = sorted(dict_temp.items(), key=lambda item: item[1], reverse=True)
                        # 여유있는 검사설비 순으로 검시시간을 계산하여 넣는다.
                        for ate in dict_temp:
                            # 최대 착공량이 0 초과일 경우에만 해당 로직을 실행.
                            if modCnt > 0:
                                # 일단 가장 여유있는 검사설비와 그 시간을 임시로 가져옴
                                tempTime = dict_ate[ate[0]]
                                ateName = ate[0]
                                # 임시 수량 컬럼에 Smt 반영 착공량을 입력
                                df_input[tempAteCnt][i] = df_input[smtReflectCnt][i]
                                if df_input[tempAteCnt][i] != 0:
                                    # 해당 검사설비능력에서 착공분만큼 삭감
                                    dict_ate[ateName] -= df_input['TotalTime'][i] * df_input[tempAteCnt][i]
                                    df_input[ateReflectCnt][i] += df_input[tempAteCnt][i]
                                    # 특수모듈인 경우에는 전체 착공랴에서 빼지 않음
                                    if df_input['특수대상'][i] != '대상':
                                        modCnt -= df_input[tempAteCnt][i]
                                    # 임시수량은 초기화
                                    df_input[tempAteCnt][i] = 0
                                    # CT사양의 경우 별도 CT제한수량에서도 삭감
                                    if '/CT' in df_input['MS Code'][i]:
                                        limitCtCnt -= df_input[tempAteCnt][i]
                                    break
                                else:
                                    break
                        # 최대착공량이 0 미만일 경우, 알람 출력
                        if modCnt < 0:
                            df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타2', df_input, i, '-', 0)
                        # CT제한대수가 0 미만일 경우, 알람 출력
                        if limitCtCnt < 0:
                            df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타4', df_input, i, '-', 0)
                            # break
                        # 검사설비능력이 0미만일 경우, 알람 출력
                        if ateName != '' and dict_ate[ateName] < 0:
                            df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '2', df_input, i, '-', math.floor((0 - dict_ate[ateName]) / df_input['TotalTime'][i]))
                            dict_ate[ateName] = 0
                        # 긴급오더 or 당일착공이 아닌 경우는 검사설비 능력을 반영하여 착공 실시
                    else:
                        # 긴급오더 처리 시, 0미만으로 떨어진 각 카운터를 0으로 초기화
                        if modCnt < 0:
                            modCnt = 0
                        if limitCtCnt < 0:
                            limitCtCnt = 0
                        # 첫 착공인지 확인하는 플래그 선언
                        isFirst = True
                        list_ate = list(df_input['INSPECTION_EQUIPMENT'][i])
                        dict_temp = {}
                        for ate in list_ate:
                            dict_temp[ate] = dict_ate[ate]
                        dict_temp = sorted(dict_temp.items(), key=lambda item: item[1], reverse=True)
                        for ate in dict_temp:
                            if tempTime <= dict_ate[ate[0]]:
                                tempTime = dict_ate[ate[0]]
                                ateName = ate[0]
                                # if ate[0] == df_input['INSPECTION_EQUIPMENT'][i][0]:
                                # 비교리스트에 [Smt반영 착공량], [최대착공량]을 입력
                                compareList = [df_input[smtReflectCnt][i], modCnt]
                                # LinkageNumber별 첫번째 착공이 아닐 경우(임시수량 있을 경우), [임시수량]도 비교리스트에 입력
                                if not isFirst:
                                    compareList.append(df_input[tempAteCnt][i])
                                # CT사양일 경우, [CT제한수량]도 비교리스트에 입력
                                if '/CT' in df_input['MS Code'][i]:
                                    compareList.append(limitCtCnt)
                                # 비교리스트 중 Min값을 임시수량으로 입력
                                df_input[tempAteCnt][i] = min(compareList)

                                if df_input[tempAteCnt][i] != 0:
                                    # 검사 설비 능력이 해당 LinkageNumber의 착공수량(임시수량)을 커버가능할 경우, 착공수량 전부를 착공대상으로 선정
                                    if dict_ate[ateName] >= df_input['TotalTime'][i] * df_input[tempAteCnt][i]:
                                        dict_ate[ateName] -= df_input['TotalTime'][i] * df_input[tempAteCnt][i]
                                        df_input[ateReflectCnt][i] += df_input[tempAteCnt][i]
                                        if df_input['특수대상'][i] != '대상':
                                            modCnt -= df_input[tempAteCnt][i]
                                        if '/CT' in df_input['MS Code'][i]:
                                            limitCtCnt -= df_input[tempAteCnt][i]
                                        df_input[tempAteCnt][i] = 0
                                        break
                                    # 검사 설비 능력이 해당 LinkageNumber의 착공수량(임시수량)을 커버 불가능할 경우, 가능한 수량까지를 착공대상으로 선정
                                    elif dict_ate[ateName] >= df_input['TotalTime'][i]:
                                        tempCnt = int(df_input[tempAteCnt][i])
                                        # 착공수량으로부터 역순으로 Loop문을 실행시켜, 검사설비 능력이 가능한 최대 한도를 확인 후, 그 수량만큼 착공대상으로 선정.
                                        for j in range(tempCnt, 0, -1):
                                            if dict_ate[ateName] >= int(df_input['TotalTime'][i]) * j:
                                                if modCnt >= j:
                                                    df_input[ateReflectCnt][i] = int(df_input[ateReflectCnt][i]) + j
                                                    dict_ate[ateName] -= int(df_input['TotalTime'][i]) * j
                                                    df_input[tempAteCnt][i] = tempCnt - j
                                                    if df_input['특수대상'][i] != '대상':
                                                        modCnt -= j
                                                    if '/CT' in df_input['MS Code'][i]:
                                                        limitCtCnt -= j
                                                    isFirst = False
                                                    break
                                # else:
                                #     break
            # CT사양의 최소필요착공량을 착공 못할 경우, 알람을 발생 시킴
            if not isRemain and (df_input[smtReflectCnt][i] > df_input[ateReflectCnt][i]):
                if '/CT' in df_input['MS Code'][i] and limitCtCnt == 0 and df_input[smtReflectCnt][i] > 0:
                    df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타4', df_input, i, '-', df_input[smtReflectCnt][i] - df_input[ateReflectCnt][i])

        return [df_input, dict_ate, alarmNo, df_alarm, modCnt, limitCtCnt]

    def fam3PowerCapaReflect(self, df_input, isRemain, dict_ratioCnt, dict_maxCnt, alarmNo, df_alarm, limitCtCnt, dict_alarmRatioCnt, dict_alarmMaxCnt, maxCnt):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_ratioCnt(Dict)         : 그룹별 제한비율 딕셔너리
            dict_maxCnt(Dict)           : 대표모델별 제한대수 딕셔너리
            alarmNo(int)                : 알람 기록용 번호
            df_alarm(DataFrame)         : 알람 상세 기록용 DataFrame
            limitCtCnt(int)             : CT 제한대수
            dict_alarmRatioCnt(Dict)    : 알람 출력용 제한비율 딕셔너리
            dict_alarmMaxCnt(int)       : 알람 출력용 대표모델별 제한대수 딕셔너리
        Return:
            return(List)
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_ratioCnt(Dict)         : 그룹별 제한비율 딕셔너리 (갱신 후)
                dict_maxCnt(Dict)           : 대표모델별 제한대수 딕셔너리 (갱신 후)
                alarmNo(int)                : 알람 기록용 번호 (갱신 후)
                df_alarm(DataFrame)         : 알람 상세 기록용 DataFrame (갱신 후)
                limitCtCnt(int)             : CT 제한대수 (갱신 후)
                dict_alarmRatioCnt(Dict)    : 알람 출력용 제한비율 딕셔너리 (갱신 후)
                dict_alarmMaxCnt(int)       : 알람 출력용 대표모델별 제한대수 딕셔너리 (갱신 후)
        """
        alarm = Alarm(self.productClass)
        instCol = 'SMT반영_착공량_잔여' if isRemain else 'SMT반영_착공량'
        resultCol1 = '설비능력반영_착공량_잔여' if isRemain else '설비능력반영_착공량'
        resultCol2 = '설비능력반영_착공공수_잔여' if isRemain else '설비능력반영_착공공수'
        for i in df_input.index:
            if df_input[instCol][i] != 0:
                # 모듈 구분 있는 경우에만 실행
                if str(df_input['상세구분'][i]) not in ['', 'nan']:
                    # 긴급오더 일 경우, 모든 조건을 무시하고 SMT반영 착공량 그대로 착공실시
                    if (str(df_input['긴급오더'][i]) == '대상' or str(df_input['당일착공'][i]) == '대상'):
                        # SMT반영 착공량을 그대로 착공하고 각 조건의 제한대수 딕셔너리에서 차감
                        df_input[resultCol2][i] = df_input[instCol][i] * df_input['공수'][i]
                        df_input[resultCol1][i] = df_input[instCol][i]
                        maxCnt -= df_input[resultCol2][i]
                        dict_ratioCnt[str(df_input['상세구분'][i])] -= float(df_input[instCol][i]) * df_input['공수'][i]
                        dict_alarmRatioCnt[str(df_input['상세구분'][i])] -= float(df_input[instCol][i]) * df_input['공수'][i]
                        if str(df_input['MAX대수'][i]) not in ['', 'nan', '-']:
                            dict_maxCnt[str(df_input['MODEL'][i])] -= float(df_input[instCol][i]) * df_input['공수'][i]
                            dict_alarmMaxCnt[str(df_input['MODEL'][i])] -= float(df_input[instCol][i]) * df_input['공수'][i]
                        # CT사양일 경우, CT제한대수를 차감
                        if '/CT' in df_input['MS Code'][i]:
                            limitCtCnt -= df_input[resultCol2][i]
                            # CT제한대수가 0 미만일 경우, 기타4 알람 기록
                            if limitCtCnt < 0:
                                df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타4', df_input, i, '-', 0 - limitCtCnt)
                        # 비율제한이 0 미만일 경우, 2-1 알람 기록
                        if dict_ratioCnt[str(df_input['상세구분'][i])] < 0:
                            df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '2-1', df_input, i, '-', 0 - (dict_ratioCnt[str(df_input['상세구분'][i])]))
                        # 모델별 제한대수가 0 미만일 경우, 2-2 알람 기록
                        if str(df_input['MAX대수'][i]) not in ['', 'nan', '-']:
                            if dict_maxCnt[str(df_input['MODEL'][i])] < 0:
                                df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '2-2', df_input, i, '-', 0 - dict_maxCnt[str(df_input['MODEL'][i])])
                        # 최대 착공량이 0 미만일 경우, 기타2 알람 기록
                        if maxCnt < 0:
                            df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타2', df_input, i, '-', 0 - maxCnt)
                    # 긴급오더 아닌 경우의 로직
                    else:
                        # 리스트에 [SMT반영 착공량], [최대 착공량], [비율제한대수] 를 입력
                        compareList = [df_input[instCol][i], maxCnt, dict_ratioCnt[str(df_input['상세구분'][i])]]
                        # 모델별 제한대수가 있는 모델이면 리스트에 [모델별 제한대수] 추가
                        if str(df_input['MAX대수'][i]) not in ['', 'nan', '-']:
                            compareList.append(dict_maxCnt[str(df_input['MODEL'][i])])
                        # CT사양일 경우, 리스트에 [CT제한대수] 추가
                        if '/CT' in df_input['MS Code'][i]:
                            compareList.append(limitCtCnt)
                        # 리스트 중, 최소값을 착공결과로 출력 / 공수도 같이 출력
                        df_input[resultCol2][i] = min(compareList)
                        df_input[resultCol1][i] = df_input[resultCol2][i] / df_input['공수'][i]
                        # 최소필요착공량 대상이며 착공 불가능한 상황인 경우, 상황에 맞는 알람을 출력
                        if not isRemain and df_input[instCol][i] > 0 and (df_input[instCol][i] != df_input[resultCol1][i]):
                            if df_input[instCol][i] > dict_alarmRatioCnt[str(df_input['상세구분'][i])]:
                                df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '2-1', df_input, i, '-', df_input[instCol][i] - dict_alarmRatioCnt[str(df_input['상세구분'][i])])
                            if str(df_input['MAX대수'][i]) != '' and str(df_input['MAX대수'][i]) != 'nan' and str(df_input['MAX대수'][i]) != '-':
                                if df_input[instCol][i] > dict_alarmMaxCnt[str(df_input['MODEL'][i])]:
                                    df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '2-2', df_input, i, '-', df_input[instCol][i] - dict_alarmMaxCnt[str(df_input['MODEL'][i])])
                            if '/CT' in df_input['MS Code'][i]:
                                if df_input[instCol][i] > limitCtCnt:
                                    df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타4', df_input, i, '-', df_input[instCol][i] - df_input[resultCol1][i])
                        # 각 조건에 맞는 딕셔너리에서 차감
                        dict_maxCnt[str(df_input['MODEL'][i])] -= df_input[resultCol2][i]
                        dict_alarmMaxCnt[str(df_input['MODEL'][i])] -= df_input[resultCol2][i]
                        if '/CT' in df_input['MS Code'][i]:
                            limitCtCnt -= df_input[resultCol2][i]
                        dict_ratioCnt[str(df_input['상세구분'][i])] -= df_input[resultCol2][i]
                        dict_alarmRatioCnt[str(df_input['상세구분'][i])] -= df_input[resultCol2][i]
                        maxCnt -= df_input[resultCol2][i]
                    # 긴급오더 등으로 0 아래로 내려가 있는 변수들을 0으로 재정의
                    if maxCnt < 0:
                        maxCnt = 0
                    if limitCtCnt < 0:
                        limitCtCnt = 0
                    if str(df_input['MAX대수'][i]) not in ['', 'nan', '-']:
                        if dict_ratioCnt[str(df_input['상세구분'][i])] < 0:
                            dict_ratioCnt[str(df_input['상세구분'][i])] = 0
                        if dict_maxCnt[str(df_input['MODEL'][i])] < 0:
                            dict_maxCnt[str(df_input['MODEL'][i])] = 0

        return [df_input, dict_ratioCnt, dict_maxCnt, alarmNo, df_alarm, limitCtCnt, dict_alarmRatioCnt, dict_alarmMaxCnt, maxCnt]

    def fam3SPCapaReflect(self, df_input, isRemain, dict_category, dict_firstGr, dict_secGr, alarmNo, df_alarm, limitCtCnt):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_category(Dict)      : 모듈/비모듈 별 잔여량 Dict
            dict_firstGr(Dict)       : 1차 Max Gr 잔여량 Dict
            dict_secGr(Dict)         : 2차 Max Gr 잔여량 Dict
            alarmNo(int)          : 알람 번호
            df_alarm(DataFrame)   : 알람 상세 기록용 DataFrame
            limitCtCnt(int)             : CT제한대수
        Return:
            return(List)
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_category(Dict)      : 모듈/비모듈 별 잔여량 Dict(갱신 후)
                dict_firstGr(Dict)       : 1차 Max Gr 잔여량 Dict(갱신 후)
                dict_secGr(Dict)         : 2차 Max Gr 잔여량 Dict(갱신 후)
                alarmNo(int)          : 알람 번호
                df_alarm(DataFrame)   : 알람 상세 기록용 DataFrame (갱신 후)
                limitCtCnt(int)             : CT제한대수
        """
        alarm = Alarm(self.productClass)
        instCol = 'SMT반영_착공량_잔여' if isRemain else 'SMT반영_착공량'
        resultCol = '설비능력반영_착공량_잔여' if isRemain else '설비능력반영_착공량'
        for i in df_input.index:
            # 긴급오더 일 경우, 모든 조건을 무시하고 착공
            if (df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상'):
                # 모듈구분 잔여 착공량이 부족한 경우, 기타2 알람 기록
                if dict_category[df_input['모듈 구분'][i]] < df_input[instCol][i] * df_input['공수'][i]:
                    df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타2', df_input, i, '-', df_input[instCol][i] * df_input['공수'][i] - dict_category[df_input['모듈 구분'][i]])
                for groupCol, dict_groupCnt in [(df_input['2차_MAX_그룹'][i], dict_secGr), (df_input['1차_MAX_그룹'][i], dict_firstGr)]:
                    if groupCol != '-':
                        if dict_groupCnt[groupCol] < df_input[instCol][i]:
                            df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '2', df_input, i, groupCol, df_input[instCol][i] - dict_groupCnt[groupCol])
                        dict_groupCnt[groupCol] -= df_input[instCol][i]
                if '/CT' in df_input['MS Code'][i]:
                    # CT사양인 경우, CT잔여량이 부족하면 기타4 알람 기록
                    if limitCtCnt < df_input[instCol][i]:
                        df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타4', df_input, i, '-', df_input[instCol][i] - limitCtCnt)
                    limitCtCnt -= df_input[instCol][i]
                df_input[resultCol][i] = df_input[instCol][i]
                dict_category[df_input['모듈 구분'][i]] -= df_input[instCol][i] * df_input['공수'][i]
            # 긴급오더 외의 대상일 경우의 로직
            else:
                # 리스트에 [SMT반영 착공량], [모듈구분 잔여 착공량 / 공수] 을 입력
                compareList = [df_input[instCol][i], (dict_category[df_input['모듈 구분'][i]] / df_input['공수'][i])]
                for groupCol, dict_groupCnt in [(df_input['2차_MAX_그룹'][i], dict_secGr), (df_input['1차_MAX_그룹'][i], dict_firstGr)]:
                    if groupCol != '-':
                        compareList.append(dict_groupCnt[groupCol])
                # CT사양일 경우, [CT제한대수]를 리스트에 입력
                if '/CT' in df_input['MS Code'][i]:
                    compareList.append(limitCtCnt)
                # 리스트 중, 최소값을 착공결과로 입력
                df_input[resultCol][i] = min(compareList)
                # 최소필요착공량이며 SMT반영 착공량을 착공할 수 없는 상황일 경우, 상황에 따른 알람을 기록
                if not isRemain and df_input[instCol][i] > 0 and (df_input[instCol][i] != df_input[resultCol][i]):
                    if df_input['1차_MAX_그룹'][i] != '-' and df_input[instCol][i] > dict_firstGr[df_input['1차_MAX_그룹'][i]]:
                        df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '2', df_input, i, df_input['1차_MAX_그룹'][i], df_input[instCol][i] - df_input[resultCol][i])
                    if df_input['2차_MAX_그룹'][i] != '-' and df_input[instCol][i] > dict_secGr[df_input['2차_MAX_그룹'][i]]:
                        df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '2', df_input, i, df_input['2차_MAX_그룹'][i], df_input[instCol][i] - df_input[resultCol][i])
                    if '/CT' in df_input['MS Code'][i] and df_input[instCol][i] > limitCtCnt:
                        df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '기타4', df_input, i, '-', df_input[instCol][i] - df_input[resultCol][i])
                # 조건에 따라 각 딕셔너리에서 착공량을 차감
                dict_category[df_input['모듈 구분'][i]] -= df_input[resultCol][i] * df_input['공수'][i]
                if df_input['1차_MAX_그룹'][i] != '-':
                    dict_firstGr[df_input['1차_MAX_그룹'][i]] -= df_input[resultCol][i]
                if df_input['2차_MAX_그룹'][i] != '-':
                    dict_secGr[df_input['2차_MAX_그룹'][i]] -= df_input[resultCol][i]
                if '/CT' in df_input['MS Code'][i]:
                    limitCtCnt -= df_input[resultCol][i]
            # 각 조건별 딕셔너리가 0 미만인 경우, 0으로 재정의
            if df_input['1차_MAX_그룹'][i] != '-' and dict_firstGr[df_input['1차_MAX_그룹'][i]] < 0:
                dict_firstGr[df_input['1차_MAX_그룹'][i]] = 0
            if df_input['2차_MAX_그룹'][i] != '-' and dict_secGr[df_input['2차_MAX_그룹'][i]] < 0:
                dict_secGr[df_input['2차_MAX_그룹'][i]] = 0
            if dict_category[df_input['모듈 구분'][i]] < 0:
                dict_category[df_input['모듈 구분'][i]] = 0
            if '/CT' in df_input['MS Code'][i] and limitCtCnt < 0:
                limitCtCnt = 0
        return [df_input, dict_category, dict_firstGr, dict_secGr, alarmNo, df_alarm, limitCtCnt]

    def juxtaCapaReflect(self, df_input, df_modelCond, isRemain, dict_gr, dict_model, dict_modelGr, dict_ate, manualInspCnt, maxCnt, alarmNo, df_alarm):
        alarm = Alarm(self.productClass)
        # [isRemain] 매개변수를 기반으로 열 이름을 결정
        instCol = 'SMT반영_착공량_잔여' if isRemain else 'SMT반영_착공량'
        resultCol = '설비능력반영_착공량_잔여' if isRemain else '설비능력반영_착공량'

        # 대상 검사설비 열과 사양번호 열의 개수를 계산
        equipmentColCnt = int(sum(df_modelCond.columns.str.startswith('대상 검사설비')))
        specColCnt = int(sum(df_input.columns.str.startswith('사양번호')))

        for i in df_input.index:
            # [긴급오더] 또는 [당일착공]에 '대상'으로 표시되어 있는지 확인
            if (df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상'):
                # 최대 착공량이 착공에 부족할 경우, 알람 발생
                if maxCnt < df_input[instCol][i]:
                    print('최대 착공량 부족 알람처리')
                maxCnt -= df_input[instCol][i]
                
                if df_input['착공비율 그룹명'][i] not in ['', '-', 'nan'] and df_input[instCol][i] in dict_gr:
                    # 그룹별 착공 비율이 부족한 경우, 알람 발생
                    if dict_gr[df_input['대표모델'][i]] < df_input[instCol][i]:
                        print('비율그룹 부족 알람처리')
                    dict_gr[df_input['대표모델'][i]] -= df_input[instCol][i]

                # 각 사양번호의 사양을 확인하고 그 사양의 일 수동검사 여부, 그룹별 최대 착공량, 모델별 최대 착공량을 적용.
                # 각 최대 착공량이 부족할 경우, 알람 발생
                for j in range(1, specColCnt):
                    specNo = df_input[f'사양번호 {str(j)}'][i]
                    modelCondRow = df_modelCond[df_modelCond['No'] == specNo]
                    if str(specNo) not in ['', 'nan', 'None']:
                        maxGroup = modelCondRow['일 최대 착공량 그룹'].values[0]
                        if maxGroup not in ['', '-', 'nan']:
                            if dict_modelGr[maxGroup] < df_input[instCol][i]:
                                print('최대 착공량 그룹 알람처리')
                            dict_modelGr[maxGroup] -= df_input[instCol][i]
                        if str(specNo) in dict_model:
                            if dict_model[specNo] < df_input[instCol][i]:
                                print('개별 모델 부족 알람처리')
                            dict_model[str(specNo)] -= df_input[instCol][i]
                        if modelCondRow['자동검사 이후 수동검사 여부'].values[0] == 'O':
                            if manualInspCnt < df_input[instCol][i]:
                                print('수동검사 수량 부족 알람처리')
                            manualInspCnt -= df_input[instCol][i]

                        dict_targetAte = {}
                        # 대상 검사설비를 확인하고 그 검사설비의 일 생산가능량을 적용.
                        for k in range(1, equipmentColCnt):
                            targetAte = modelCondRow[f'대상 검사설비 {str(k)}'].values[0]
                            if str(targetAte) not in ['', '-', 'nan', 'None']:
                                dict_targetAte[targetAte] = dict_ate[targetAte]

                        if bool(dict_targetAte):
                            # 가장 여유있는 검사설비를 확인.
                            keyAte = max(dict_targetAte, key=dict_targetAte.get)
                            # 해당 검사설비가 여유없는 경우에는 타 검사설비를 확인하고 합하여 생산가능한지 확인.
                            if dict_ate[keyAte] < df_input[instCol][i]:
                                shortage = df_input[instCol][i] - dict_ate[keyAte]
                                for key, value in dict_ate.items():
                                    if key != keyAte and value >= shortage:
                                        dict_ate[key] -= shortage
                                        dict_ate[keyAte] += shortage
                                        break
                                    elif key != keyAte:
                                        dict_ate[keyAte] += value
                                        shortage -= value
                                        dict_ate[key] = 0
                                        if shortage <= 0:
                                            break
                                if shortage > 0:
                                    print('검사설비 부족 알람처리')
                            dict_ate[keyAte] -= df_input[instCol][i]
                df_input[resultCol][i] = df_input[instCol][i]
            else:
                # 비교리스트에 [Smt반영 착공량], [최대 착공량]을 추가
                compareList = [df_input[instCol][i], maxCnt]

                # 그룹별 착공비율이 존재할 경우, 비교리스트에 추가
                if df_input['착공비율 그룹명'][i] not in ['', '-', 'nan'] and df_input[instCol][i] in dict_gr:
                    compareList.append(dict_gr[df_input['착공비율 그룹명'][i]])

                # 모델별 사양번호를 체크 후, 수동검사 여부 / 그룹별 최대 착공량 / 모델별 최대 착공량을 비교리스트에 추가
                for j in range(1, specColCnt):
                    specNo = df_input[f'사양번호 {str(j)}'][i]
                    modelCondRow = df_modelCond[df_modelCond['No'] == specNo]
                    if str(specNo) not in ['', 'nan', 'None']:
                        maxGroup = modelCondRow['일 최대 착공량 그룹'].values[0]
                        if maxGroup not in ['', '-', 'nan']:
                            compareList.append(dict_modelGr[maxGroup])
                        if str(specNo) in dict_model:
                            compareList.append(dict_model[specNo])
                        if modelCondRow['자동검사 이후 수동검사 여부'].values[0] == 'O':
                            compareList.append(manualInspCnt)

                        dict_targetAte = {}
                        # 대상 검사설비 확인 후, 검사설비별 최대 착공량을 비교리스트에 추가
                        for k in range(1, equipmentColCnt):
                            targetAte = modelCondRow[f'대상 검사설비 {str(k)}'].values[0]
                            if str(targetAte) not in ['', '-', 'nan', 'None']:
                                dict_targetAte[targetAte] = dict_ate[targetAte]
                        
                        if bool(dict_targetAte):
                            # 가장 여유있는 검사설비를 확인.
                            keyAte = max(dict_targetAte, key=dict_targetAte.get)
                            # 해당 검사설비가 여유없는 경우에는 타 검사설비를 확인하고 합하여 생산가능한지 확인.
                            if dict_ate[keyAte] < df_input[instCol][i]:
                                shortage = df_input[instCol][i] - dict_ate[keyAte]
                                for key, value in dict_ate.items():
                                    if key != keyAte and value >= shortage:
                                        dict_ate[key] -= shortage
                                        dict_ate[keyAte] += shortage
                                        break
                                    elif key != keyAte:
                                        dict_ate[keyAte] += value
                                        shortage -= value
                                        dict_ate[key] = 0
                                        if shortage <= 0:
                                            break
                            compareList.append(dict_ate[keyAte])

                df_input[resultCol][i] = min(compareList)
                maxCnt -= df_input[resultCol][i]

                # 최소필요착공량이며 SMT반영 착공량을 착공할 수 없는 상황일 경우, 상황에 따른 알람을 기록
                if not isRemain and df_input[instCol][i] > 0 and (df_input[instCol][i] != df_input[resultCol][i]):
                    if df_input[instCol][i] > maxCnt:
                        print('최대 착공량 부족 알람처리')
                    if df_input['착공비율 그룹명'][i] not in ['', '-', 'nan'] and df_input[instCol][i] > dict_gr[df_input['착공비율 그룹명'][i]]:
                        print('비율그룹 부족 알람처리')
                    for j in range(1, specColCnt):
                        specNo = df_input[f'사양번호 {str(j)}'][i]
                        modelCondRow = df_modelCond[df_modelCond['No'] == specNo]
                        if str(specNo) not in ['', 'nan', 'None']:
                            maxGroup = modelCondRow['일 최대 착공량 그룹'].values[0]
                            if maxGroup not in ['', '-', 'nan'] and dict_modelGr[maxGroup] < df_input[instCol][i]:
                                df_alarm, alarmNo = alarm.concatAlarmDetail(df_alarm, alarmNo, '2', df_input, i, maxGroup, df_input[instCol][i] - df_input[resultCol][i])
                                # print(f"최대 착공량 그룹 알람 {df_input['Linkage Number'][i]}, {df_input['MS Code'][i]}, 미착공수주잔 : {df_input[instCol][i]}, {maxGroup}")
                            if str(specNo) in dict_model and dict_model[specNo] < df_input[instCol][i]:
                                print('개별 모델 부족 알람처리')
                            if modelCondRow['자동검사 이후 수동검사 여부'].values[0] == 'O' and manualInspCnt < df_input[instCol][i]:
                                print('수동검사 수량 부족 알람처리')
                            if bool(dict_targetAte):
                                keyAte = max(dict_targetAte, key=dict_targetAte.get)
                                if dict_ate[keyAte] < df_input[instCol][i]:
                                    print('검사설비 능력 부족 알람처리')

                if df_input['착공비율 그룹명'][i] not in ['', '-', 'nan'] and df_input[instCol][i] in dict_gr:
                    dict_gr[df_input['착공비율 그룹명'][i]] -= df_input[resultCol][i]

                for j in range(1, specColCnt):
                    specNo = df_input[f'사양번호 {str(j)}'][i]
                    modelCondRow = df_modelCond[df_modelCond['No'] == specNo]
                    if str(specNo) not in ['', 'nan', 'None']:
                        maxGroup = modelCondRow['일 최대 착공량 그룹'].values[0]
                        if maxGroup not in ['', '-', 'nan']:
                            dict_modelGr[maxGroup] -= df_input[resultCol][i]
                        if specNo in dict_model:
                            dict_model[specNo] -= df_input[resultCol][i]
                        if modelCondRow['자동검사 이후 수동검사 여부'].values[0] == 'O':
                            manualInspCnt -= df_input[resultCol][i]

                        if bool(dict_targetAte):
                            keyAte = max(dict_targetAte, key=dict_targetAte.get)
                            dict_ate[keyAte] -= df_input[resultCol][i]

            if df_input['착공비율 그룹명'][i] not in ['', '-', 'nan'] and df_input[instCol][i] in dict_gr and dict_gr[df_input['대표모델'][i]] < 0:
                dict_gr[df_input['대표모델'][i]] = 0
            for j in range(1, specColCnt):
                specNo = df_input[f'사양번호 {str(j)}'][i]
                modelCondRow = df_modelCond[df_modelCond['No'] == specNo]
                if str(specNo) not in ['', 'nan', 'None']:
                    maxGroup = modelCondRow['일 최대 착공량 그룹'].values[0]
                    if maxGroup not in ['', '-', 'nan']:
                        if dict_modelGr[maxGroup] < 0:
                            dict_modelGr[maxGroup] = 0
                    if str(specNo) in dict_model:
                        if dict_model[specNo] < 0:
                            dict_model[specNo] = 0
                    if modelCondRow['자동검사 이후 수동검사 여부'].values[0] == 'O':
                        if manualInspCnt < 0:
                            manualInspCnt = 0
                    if bool(dict_targetAte):
                        for key, value in dict_targetAte.items():
                            if dict_ate[key] < 0:
                                dict_ate[key] = 0

        return df_input, dict_gr, dict_model, dict_modelGr, dict_ate, manualInspCnt, maxCnt, df_alarm, alarmNo

    def loadLevelingList(self):
        if self.className == 'Fam3MainThread' or self.className == 'JuxtaOldThread':
            df_leveling = pd.read_excel(self.list_masterFile[1])
        elif self.className == 'Fam3SpThread':
            df_leveling = pd.read_excel(self.list_masterFile[2])
        elif self.className == 'Fam3PowerThread':
            df_leveling = pd.read_excel(self.list_masterFile[3])
        df_constDate = df_leveling[df_leveling['Scheduled Start Date (*)'] == self.constDate]
        df_constDate = df_constDate[df_constDate['Sequence No'].notnull()]
        if len(df_constDate) > 0:
            df_constDate = df_constDate[df_constDate['Sequence No'].str.contains('D0')]
        if len(df_constDate) > 0:
            maxNo = df_constDate['No (*)'].max()
        else:
            maxNo = 0
        # 미착공 대상만 추출()
        df_levelingDropSeq = df_leveling[df_leveling['Sequence No'].isnull()]
        df_levelingUndepSeq = df_leveling[df_leveling['Sequence No'] == 'Undep']
        df_levelingUncorSeq = df_leveling[df_leveling['Sequence No'] == 'Uncor']
        df_leveling = pd.concat([df_levelingDropSeq, df_levelingUndepSeq, df_levelingUncorSeq])
        df_leveling['Linkage Number'] = df_leveling['Linkage Number'].astype(str)
        df_leveling = df_leveling.reset_index(drop=True)
        df_leveling['미착공수주잔'] = df_leveling.groupby('Linkage Number')['Linkage Number'].transform('size')
        if self.className == 'Fam3MainThread':
            df_leveling['특수대상'] = ''
            df_ateP = self.loaddfAteP()
            if Path(self.list_masterFile[2]).is_file():
                df_levelingSp = pd.read_excel(self.list_masterFile[2])
                # 미착공 대상만 추출(특수_모듈)
                df_levelingSpDropSeq = df_levelingSp[df_levelingSp['Sequence No'].isnull()]
                df_levelingSpUndepSeq = df_levelingSp[df_levelingSp['Sequence No'] == 'Undep']
                df_levelingSpUncorSeq = df_levelingSp[df_levelingSp['Sequence No'] == 'Uncor']
                df_levelingSp = pd.concat([df_levelingSpDropSeq, df_levelingSpUndepSeq, df_levelingSpUncorSeq])
                df_levelingSp['대표모델6자리'] = df_levelingSp['MS-CODE'].str[:6]
                df_levelingSp = pd.merge(df_levelingSp, df_ateP, how='right', left_on='대표모델6자리', right_on='MODEL')
                df_levelingSp['Linkage Number'] = df_levelingSp['Linkage Number'].astype(str)
                df_levelingSp = df_levelingSp.reset_index(drop=True)
                df_levelingSp['미착공수주잔'] = df_levelingSp.groupby('Linkage Number')['Linkage Number'].transform('size')
        elif self.className == 'Fam3SpThread':
            df_leveling['모듈 구분'] = '모듈'
            df_cond = pd.read_excel(self.list_masterFile[7])
            df_cond['No'] = df_cond['No'].fillna(method='ffill')
            df_cond['1차_MAX_그룹'] = df_cond['1차_MAX_그룹'].fillna(method='ffill')
            df_cond['2차_MAX_그룹'] = df_cond['2차_MAX_그룹'].fillna(method='ffill')
            df_cond['1차_MAX'] = df_cond['1차_MAX'].fillna(method='ffill')
            df_cond['2차_MAX'] = df_cond['2차_MAX'].fillna(method='ffill')
            # 비모듈 레벨링 리스트 불러오기 - 경로에 파일이 있으면 불러올것
            if self.cb_round == '2차':
                if Path(self.list_masterFile[9]).is_file():
                    df_levelingBL = pd.read_excel(self.list_masterFile[9])
                    df_constDateBL = df_levelingBL[df_levelingBL['Scheduled Start Date (*)'] == self.constDate]
                    df_constDateBL = df_constDateBL[df_constDateBL['Sequence No'].notnull()]
                    if len(df_constDateBL) > 0:
                        df_constDateBL = df_constDateBL[df_constDateBL['Sequence No'].str.contains('D0')]
                    if len(df_constDateBL) > 0:
                        maxNoBL = df_constDateBL['No (*)'].max()
                    else:
                        maxNoBL = 0
                    df_levelingBLDropSeq = df_levelingBL[df_levelingBL['Sequence No'].isnull()]
                    df_levelingBLUndepSeq = df_levelingBL[df_levelingBL['Sequence No'] == 'Undep']
                    df_levelingBLUncorSeq = df_levelingBL[df_levelingBL['Sequence No'] == 'Uncor']
                    df_levelingBL = pd.concat([df_levelingBLDropSeq, df_levelingBLUndepSeq, df_levelingBLUncorSeq])
                    df_levelingBL['모듈 구분'] = df_cond[df_cond['상세구분'] == 'BL=Case']['구분'].values[0]
                    df_levelingBL['Linkage Number'] = df_levelingBL['Linkage Number'].astype(str)
                    df_levelingBL = df_levelingBL.reset_index(drop=True)
                    df_levelingBL['미착공수주잔'] = df_levelingBL.groupby('Linkage Number')['Linkage Number'].transform('size')
                    df_leveling = pd.concat([df_leveling, df_levelingBL])
                else:
                    maxNoBL = 0
                if Path(self.list_masterFile[10]).is_file():
                    df_levelingTerminal = pd.read_excel(self.list_masterFile[10])
                    df_constDateTerminal = df_levelingTerminal[df_levelingTerminal['Scheduled Start Date (*)'] == self.constDate]
                    df_constDateTerminal = df_constDateTerminal[df_constDateTerminal['Sequence No'].notnull()]
                    if len(df_constDateTerminal) > 0:
                        df_constDateTerminal = df_constDateTerminal[df_constDateTerminal['Sequence No'].str.contains('D0')]
                    if len(df_constDateTerminal) > 0:
                        maxNoTerminal = df_constDateTerminal['No (*)'].max()
                    else:
                        maxNoTerminal = 0
                    df_levelingTerminalDropSeq = df_levelingTerminal[df_levelingTerminal['Sequence No'].isnull()]
                    df_levelingTerminalUndepSeq = df_levelingTerminal[df_levelingTerminal['Sequence No'] == 'Undep']
                    df_levelingTerminalUncorSeq = df_levelingTerminal[df_levelingTerminal['Sequence No'] == 'Uncor']
                    df_levelingTerminal = pd.concat([df_levelingTerminalDropSeq, df_levelingTerminalUndepSeq, df_levelingTerminalUncorSeq])
                    df_levelingTerminal['모듈 구분'] = df_cond[df_cond['상세구분'] == 'Terminal']['구분'].values[0]
                    df_levelingTerminal['Linkage Number'] = df_levelingTerminal['Linkage Number'].astype(str)
                    df_levelingTerminal = df_levelingTerminal.reset_index(drop=True)
                    df_levelingTerminal['미착공수주잔'] = df_levelingTerminal.groupby('Linkage Number')['Linkage Number'].transform('size')
                    df_leveling = pd.concat([df_leveling, df_levelingTerminal])
                else:
                    maxNoTerminal = 0
            elif self.cb_round == '1차':
                maxNoBL = 0
                maxNoTerminal = 0
                if Path(self.list_masterFile[11]).is_file():
                    df_levelingSlave = pd.read_excel(self.list_masterFile[11])
                    df_constDateSlave = df_levelingSlave[df_levelingSlave['Scheduled Start Date (*)'] == self.constDate]
                    df_constDateSlave = df_constDateSlave[df_constDateSlave['Sequence No'].notnull()]
                    if len(df_constDateSlave) > 0:
                        df_constDateSlave = df_constDateSlave[df_constDateSlave['Sequence No'].str.contains('D0')]
                    if len(df_constDateSlave) > 0:
                        maxNoSlave = df_constDateSlave['No (*)'].max()
                    else:
                        maxNoSlave = 0
                    df_levelingSlaveDropSeq = df_levelingSlave[df_levelingSlave['Sequence No'].isnull()]
                    df_levelingSlaveUndepSeq = df_levelingSlave[df_levelingSlave['Sequence No'] == 'Undep']
                    df_levelingSlaveUncorSeq = df_levelingSlave[df_levelingSlave['Sequence No'] == 'Uncor']
                    df_levelingSlave = pd.concat([df_levelingSlaveDropSeq, df_levelingSlaveUndepSeq, df_levelingSlaveUncorSeq])
                    df_levelingSlave['모듈 구분'] = df_cond[df_cond['상세구분'] == 'Slave']['구분'].values[0]
                    df_levelingSlave['Linkage Number'] = df_levelingSlave['Linkage Number'].astype(str)
                    df_levelingSlave = df_levelingSlave.reset_index(drop=True)
                    df_levelingSlave['미착공수주잔'] = df_levelingSlave.groupby('Linkage Number')['Linkage Number'].transform('size')
                    df_leveling = pd.concat([df_leveling, df_levelingSlave])
                else:
                    maxNoSlave = 0
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'JuxtaOldThread':
            return df_leveling, maxNo, 0, 0, 0
        elif self.className == 'Fam3SpThread':
            return df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave

    def preProcessing(self, df_leveling):
        # 긴급오더, 홀딩오더 불러오기
        self.productClass
        emgLinkage = self.emgHoldList[0]
        emgmscode = self.emgHoldList[1]
        holdLinkage = self.emgHoldList[2]
        holdmscode = self.emgHoldList[3]
        df_emgLinkage = pd.DataFrame({'Linkage Number': emgLinkage})
        df_emgmscode = pd.DataFrame({'MS Code': emgmscode})
        df_holdLinkage = pd.DataFrame({'Linkage Number': holdLinkage})
        df_holdmscode = pd.DataFrame({'MS Code': holdmscode})
        df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(np.int64)
        df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(np.int64)
        # 긴급오더, 홍딩오더 Join 전 컬럼 추가
        df_emgLinkage['긴급오더'] = '대상'
        df_emgmscode['긴급오더'] = '대상'
        df_holdLinkage['홀딩오더'] = '대상'
        df_holdmscode['홀딩오더'] = '대상'

        # 공통로직
        df_sosFile = pd.read_excel(self.list_masterFile[0])
        if self.className == 'JuxtaOldThread':
            df_sosFile['リンケージナンバー'] = df_sosFile['リンケージナンバー'].astype(str)
            df_sosFileMerge = pd.merge(df_sosFile, df_leveling, left_on='リンケージナンバー', right_on='Linkage Number').drop_duplicates(['リンケージナンバー'])

        else:
            df_sosFile['Linkage Number'] = df_sosFile['Linkage Number'].astype(str)
            # 착공 대상 외 모델 삭제
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('ZOTHER')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('YZ')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('SF')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('KM')].index)
            df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('TA80')].index)
            # CT사양은 1차에서만 착공내리도록 처리
            if self.cb_round != '1차':
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('CT')].index)

            # 진척 파일 - SOS2파일 Join
            df_sosFileMerge = pd.merge(df_sosFile, df_leveling).drop_duplicates(['Linkage Number'])
        if self.className == 'Fam3MainThread':
            # if Path(self.list_masterFile[2]).is_file():
            #     df_sosFileMergeSp = pd.merge(df_sosFile, df_levelingSp).drop_duplicates(['Linkage Number'])
            #     df_sosFileMerge = pd.concat([df_sosFileMerge, df_sosFileMergeSp])
            # else:
            df_sosFileMerge['1차_MAX_그룹'] = ''
            df_sosFileMerge['2차_MAX_그룹'] = ''
            df_sosFileMerge['1차_MAX'] = ''
            df_sosFileMerge['2차_MAX'] = ''
            df_sosFileMerge['우선착공'] = ''
            df_sosFileMerge = df_sosFileMerge[['Linkage Number',
                                                'MS Code',
                                                'Planned Prod. Completion date',
                                                'Order Quantity',
                                                '미착공수주잔',
                                                '특수대상',
                                                '우선착공',
                                                '1차_MAX_그룹',
                                                '2차_MAX_그룹',
                                                '1차_MAX',
                                                '2차_MAX']]
        elif self.className == 'Fam3PowerThread':
            df_sosFileMerge = df_sosFileMerge[['Linkage Number', 'MS Code', 'Planned Prod. Completion date', 'Order Quantity', '미착공수주잔']]
        elif self.className == 'Fam3SpThread':
            df_sosFileMerge = df_sosFileMerge[['Linkage Number', 'MS Code', 'Planned Prod. Completion date', 'Order Quantity', '미착공수주잔', '모듈 구분']]
        elif self.className == 'JuxtaOldThread':
            df_sosFileMerge = df_sosFileMerge[['リンケージナンバー', 'MSコード', '完成予定日', '受注数量', '特注決裁番号', 'XJNo.', '미착공수주잔']]
            df_sosFileMerge.columns = ['Linkage Number', 'MS Code', 'Planned Prod. Completion date', 'Order Quantity', 'XJNo.', '특주결재번호', '미착공수주잔']

        # 미착공수주잔이 없는 데이터는 불요이므로 삭제
        df_sosFileMerge = df_sosFileMerge[df_sosFileMerge['미착공수주잔'] != 0]
        # 위 파일을 완성지정일 기준 오름차순 정렬 및 인덱스 재설정
        df_sosFileMerge = df_sosFileMerge.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
        df_sosFileMerge = df_sosFileMerge.reset_index(drop=True)
        # 대표모델 Column 생성
        if self.className == 'JuxtaOldThread':
            df_sosFileMerge['대표모델'] = df_sosFileMerge['MS Code'].str.split('-', 1).str[0].str.split('/', 1).str[0]
        else:
            df_sosFileMerge['대표모델'] = df_sosFileMerge['MS Code'].str[:9]
        # 남은 워킹데이 Column 생성
        df_sosFileMerge['남은 워킹데이'] = 0
        # 긴급오더, 홀딩오더 Linkage Number Column 타입 일치
        df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(str)
        df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(str)
        # 긴급오더, 홀딩오더와 위 Sos파일을 Join
        df_mergeLink = pd.merge(df_sosFileMerge, df_emgLinkage, on='Linkage Number', how='left')
        df_Mergemscode = pd.merge(df_sosFileMerge, df_emgmscode, on='MS Code', how='left')
        df_mergeLink = pd.merge(df_mergeLink, df_holdLinkage, on='Linkage Number', how='left')
        df_Mergemscode = pd.merge(df_Mergemscode, df_holdmscode, on='MS Code', how='left')
        df_mergeLink['긴급오더'] = df_mergeLink['긴급오더'].combine_first(df_Mergemscode['긴급오더'])
        df_mergeLink['홀딩오더'] = df_mergeLink['홀딩오더'].combine_first(df_Mergemscode['홀딩오더'])
        df_mergeLink['당일착공'] = ''
        df_mergeLink['완성지정일_원본'] = df_mergeLink['Planned Prod. Completion date']
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SpThread':
            # CT사양은 기존 완성지정일보다 4일 더 빠르게 착공내려야 하기 때문에 보정처리
            df_mergeLink.loc[df_mergeLink['MS Code'].str.contains('/CT'), 'Planned Prod. Completion date'] = df_mergeLink['완성지정일_원본'] - datetime.timedelta(days=4)
        df_mergeLink = df_mergeLink.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
        df_mergeLink = df_mergeLink.reset_index(drop=True)
        if self.className == 'JuxtaOldThread':
            df_mergeLink['특주대상'] = ''
            df_mergeLink.loc[df_mergeLink['MS Code'].str.contains('/Z'), '특주대상'] = '대상'
            df_mergeLink.loc[df_mergeLink['XJNo.'].notnull(), '특주대상'] = '대상'

            df_alrdyPrtTag = pd.read_excel(self.list_masterFile[6], sheet_name=1)
            df_alrdyPrtTag['LINKAGE NO'] = df_alrdyPrtTag['LINKAGE NO'].astype(str)
            df_alrdyPrtTag['기출력 대상'] = '대상'
            df_mergeLink = pd.merge(df_mergeLink, df_alrdyPrtTag, how='left', left_on='Linkage Number', right_on='LINKAGE NO')

            df_holdList = pd.read_excel(self.list_masterFile[6], sheet_name=0)
            df_holding = df_holdList[df_holdList['LINKAGENO'].notnull()]
            df_holding['홀딩대상'] = '대상'
            df_job = df_holdList[df_holdList['MODEL'].notnull()]
            df_job['JOB구분대상'] = '대상'
            df_spDelivery = df_holdList[df_holdList['특주결재 NO'].notnull()]
            df_spDelivery['특주결재대상'] = '대상'
            df_holding['LINKAGENO'] = df_holding['LINKAGENO'].astype(str)
            df_mergeLink = pd.merge(df_mergeLink, df_holding[['LINKAGENO', '홀딩대상', 'REMARK']], how='left', left_on='Linkage Number', right_on='LINKAGENO')
            df_mergeLink = pd.merge(df_mergeLink, df_job[['MODEL', 'JOB구분대상', 'JOB구분일', 'REMARK']], how='left', left_on='대표모델', right_on='MODEL')
            df_mergeLink['특주결재 NO'] = df_sosFileMerge['특주결재번호'].str.split('-', 1).str[0]
            df_mergeLink = pd.merge(df_mergeLink, df_spDelivery[['특주결재 NO', '특주결재대상', 'REMARK']], how='left', on='특주결재 NO')
            df_mergeLink['REMARK'] = df_mergeLink['REMARK'].fillna(df_mergeLink['REMARK_x']).fillna(df_mergeLink['REMARK_y'])
            df_mergeLink = df_mergeLink.drop(columns=['REMARK_x', 'REMARK_y'])
            df_mergeLink = df_mergeLink[df_mergeLink['홀딩대상'] != '대상']
            df_mergeLink['남은JOB구분일'] = 0
            df_calendar = self.neuronDb.readDB("SELECT WC_DATE, WC_GUBUN FROM WK_CALENDAR WHERE WC_DATE >= (SYSDATE - 365) ORDER BY WC_DATE ASC")
            df_calendar['WC_GUBUN'] = df_calendar['WC_GUBUN'].astype(np.int64)
            workDayCalculator = WorkDayCalculator()
            for i in df_mergeLink.index:
                if df_mergeLink['JOB구분대상'][i] == '대상':
                    if str(df_mergeLink['JOB구분일'][i]) not in 'nan':
                        df_mergeLink['남은JOB구분일'][i] = workDayCalculator.checkWorkDay(df_calendar, self.date, df_mergeLink['JOB구분일'][i])

        return df_mergeLink

    def postProcessing(self, df, df_levelingCompDate):
        if self.className == 'Fam3MainThread':
            fam3ProductDb = self.loadFam3ProductDB()
            df_productTime = fam3ProductDb.readDB('SELECT * FROM FAM3_PRODUCT_TIME_TB')
            # 전체 검사시간을 계산
            df_productTime['TotalTime'] = (df_productTime['M_FUNCTION_CHECK'].apply(self.getSec) + df_productTime['A_FUNCTION_CHECK'].apply(self.getSec))
            df_productTime['대표모델'] = df_productTime['MODEL'].str[:9]
            df_productTime = df_productTime.drop_duplicates(['대표모델'])
            df_productTime = df_productTime.reset_index(drop=True)
            df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].apply(self.delBackslash)
            df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].str.strip()
            df_levelingCompDate = pd.merge(df_levelingCompDate, df_productTime[['대표모델', 'TotalTime', 'INSPECTION_EQUIPMENT']], on='대표모델', how='left')
            addColumnList = ['평준화_적용_착공량', '잔여_착공량', 'SMT반영_착공량', 'SMT반영_착공량_잔여', '설비능력반영_착공량', '설비능력반영_착공량_잔여']
            # 20개씩 분할되었던 Row를 하나로 통합하는 작업 실시
            for column in addColumnList: 
                df_group = df.groupby('Linkage Number')[column].sum()
                df_group = df_group.reset_index()
                df_group.columns = ['Linkage Number', column]
                df_levelingCompDate = pd.merge(df_levelingCompDate, df_group[['Linkage Number', column]], on='Linkage Number', how='left')
                df_levelingCompDate[column] = df_levelingCompDate[column].fillna(0)
            df = df_levelingCompDate.copy()
            df = df.sort_values(by=['우선착공', '긴급오더', '당일착공', 'Planned Prod. Completion date', '설비능력반영_착공량', '설비능력반영_착공량_잔여'], ascending=[False, False, False, True, False, False])
            df = df.reset_index(drop=True)
            df['MODEL'] = df['MS Code'].str[:6]
            # 총착공량 컬럼으로 병합
            df['총착공량'] = df['설비능력반영_착공량'] + df['설비능력반영_착공량_잔여']
            df = df[df['총착공량'] != 0]
            # 홀딩리스트 파일 불러오기
            df_holdingList = pd.read_excel(self.list_masterFile[17])
            # 홀딩리스트와 비교하여 조건에 해당하는 경우, 알람 메시지 출력
            for i in df_holdingList.index:
                message = ""
                if len(df[df['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values) > 0:
                    totalCnt = 0
                    for cnt in df[df['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values:
                        totalCnt += cnt
                    message = f"{df_holdingList['MODEL'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                if len(df[df['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values) > 0:
                    message = f"{df_holdingList['LINKAGENO'][i]} {int(df[df['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values[0])}대 {df_holdingList['REMARK'][i]}"
                if len(df[df['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values) > 0:
                    totalCnt = 0
                    for cnt in df[df['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values:
                        totalCnt += cnt
                    message = f"{df_holdingList['MS-CODE'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                # if len(message) > 0:
                #     self.mainReturnWarning.emit(message)
            # df_returnSp = df[df['특수대상'] == '대상']
            # self.mainReturnDf.emit(df_returnSp)
            df = df[df['특수대상'] != '대상']
            # 최대착공량만큼 착공 못했을 경우, 메시지 출력
            # if self.moduleMaxCnt > 0:
            #     self.mainReturnWarning.emit(f'아직 착공하지 못한 모델이 [{int(self.moduleMaxCnt)}대] 남았습니다. 설비능력 부족이 예상됩니다. 확인해주세요.')
        elif self.className == 'Fam3PowerThread':
            # 총착공량 컬럼으로 병합
            df['총착공량'] = df['설비능력반영_착공량'] + df['설비능력반영_착공량_잔여']
            df = df[df['총착공량'] != 0]
            df['MODEL'] = df['MS Code'].str[:6]
            # 홀딩리스트 불러오기
            df_holdingList = pd.read_excel(self.list_masterFile[17])
            # 홀딩리스트와 비교하여 조건에 해당하는 경우, 알람 메시지 출력
            for i in df_holdingList.index:
                message = ""
                if len(df[df['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values) > 0:
                    totalCnt = 0
                    for cnt in df[df['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values:
                        totalCnt += cnt
                    message = f"{df_holdingList['MODEL'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                if len(df[df['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values) > 0:
                    message = f"{df_holdingList['LINKAGENO'][i]} {int(df[df['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values[0])}대 {df_holdingList['REMARK'][i]}"
                if len(df[df['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values) > 0:
                    totalCnt = 0
                    for cnt in df[df['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values:
                        totalCnt += cnt
                    message = f"{df_holdingList['MS-CODE'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                # if len(message) > 0:
                #     self.powerReturnWarning.emit(message)
            # 최대착공량만큼 착공 못했을 경우, 메시지 출력
            # if math.floor(self.moduleMaxCnt) > 0:
            #     self.powerReturnWarning.emit(f'아직 착공하지 못한 모델이 [{math.floor(self.moduleMaxCnt)}대] 남았습니다. 모델별 제한 대수 혹은 최대 착공량 의 설정 이상이 예상됩니다. 확인해주세요.')
        elif self.className == 'Fam3SpThread':
            # 총착공량 컬럼으로 병합
            df['총착공량'] = df['설비능력반영_착공량'] + df['설비능력반영_착공량_잔여']
            df = df[df['총착공량'] != 0]
            # 홀딩리스트 불러오기
            df_holdingList = pd.read_excel(self.list_masterFile[17])
            # 홀딩리스트와 비교하여 조건에 해당하는 경우, 알람 메시지 출력
            for i in df_holdingList.index:
                message = ""
                if len(df[df['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values) > 0:
                    totalCnt = 0
                    for cnt in df[df['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values:
                        totalCnt += cnt
                    message = f"{df_holdingList['MODEL'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                if len(df[df['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values) > 0:
                    message = f"{df_holdingList['LINKAGENO'][i]} {int(df[df['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values[0])}대 {df_holdingList['REMARK'][i]}"
                if len(df[df['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values) > 0:
                    totalCnt = 0
                    for cnt in df[df['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values:
                        totalCnt += cnt
                    message = f"{df_holdingList['MS-CODE'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                # if len(message) > 0:
                #     self.spReturnWarning.emit(message)
            # 최대착공량만큼 착공 못했을 경우, 메시지 출력
            # if math.floor(dict_categoryCnt['모듈']) > 0:
            #     self.spReturnWarning.emit(f'아직 착공하지 못한 특수(모듈)이 [{math.floor(dict_categoryCnt["모듈"])}대] 남았습니다. 최대 생산대수 설정을 확인해주세요.')
            # if math.floor(dict_categoryCnt['비모듈']) > 0:
            #     self.spReturnWarning.emit(f'아직 착공하지 못한 특수(비모듈)이 [{math.floor(dict_categoryCnt["비모듈"])}대] 남았습니다. 레벨링 리스트 파일 혹은 최대 생산대수 설정을 확인해주세요.')
        elif self.className == 'JuxtaOldThread':
            df['총착공량'] = df['설비능력반영_착공량'] + df['설비능력반영_착공량_잔여']
            df = df[df['총착공량'] != 0]
            message = ''
        
        return df, message

    def addSmtAssyInfo(self, df):
        # 프로그램 기동날짜의 전일을 계산 (Debug시에는 디버그용 LineEdit에 기록된 날짜를 사용)
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        if self.isDebug:
            yesterday = (datetime.datetime.strptime(self.date, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
        # 설정파일 불러오기
        smtAssyDbHost = self.parser.get('SMT Assy DB정보', 'Host')
        smtAssyDbPort = self.parser.getint('SMT Assy DB정보', 'Port')
        smtAssyDbSID = self.parser.get('SMT Assy DB정보', 'SID')
        smtAssyDbUser = self.parser.get('SMT Assy DB정보', 'Username')
        smtAssyDbPw = self.parser.get('SMT Assy DB정보', 'Password')
        smtAssyDb = OracleDBReader(smtAssyDbHost, smtAssyDbPort, smtAssyDbSID, smtAssyDbUser, smtAssyDbPw)
        # 해당 날짜의 Smt Assy 남은 대수 확인
        df_smtAssyInven = smtAssyDb.readDB("SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE(" + str(yesterday) + ",'YYYYMMDD')")
        df_smtAssyInven['현재수량'] = 0
        # 2차 메인피킹 리스트 불러오기 및 Smt Assy 재고량 Df와 Join
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SpThread':
            if Path(self.list_masterFile[5]).is_file() or Path(self.list_masterFile[14]).is_file() or Path(self.list_masterFile[15]).is_file():
                df_secOrderList = pd.DataFrame(columns=['ASSY NO', '대수', 'SMT STORE ADDRESS'])
                if Path(self.list_masterFile[5]).is_file():
                    df_secOrderMainList = pd.read_excel(self.list_masterFile[5], skiprows=5)
                    df_secOrderList = pd.concat([df_secOrderList, df_secOrderMainList])
                if Path(self.list_masterFile[14]).is_file():
                    df_secOrderPowerList = pd.read_excel(self.list_masterFile[14], skiprows=5)
                    df_secOrderList = pd.concat([df_secOrderList, df_secOrderPowerList])
                if Path(self.list_masterFile[15]).is_file():
                    df_secOrderSpList = pd.read_excel(self.list_masterFile[15], skiprows=5)
                    df_secOrderList = pd.concat([df_secOrderList, df_secOrderSpList])
                df_joinSmt = pd.merge(df_secOrderList, df_smtAssyInven, how='right', left_on='ASSY NO', right_on='PARTS_NO')
                df_joinSmt['대수'] = df_joinSmt['대수'].fillna(0)
                # Smt Assy 현재 재고량에서 사용량 차감
                df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY'] - df_joinSmt['대수']
            else:
                df_joinSmt = df_smtAssyInven.copy()
                df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY']
        elif self.className == 'JuxtaOldThread':
            df_joinSmt = df_smtAssyInven.copy()
            df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY']
        dict_smtCnt = {}
        # Smt Assy 재고량을 PARTS_NO를 Key로 Dict화
        for i in df_joinSmt.index:
            if df_joinSmt['현재수량'][i] < 0:
                df_joinSmt['현재수량'][i] = 0
            dict_smtCnt[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]
        if self.className == 'Fam3MainThread':
            df_ateP = self.loaddfAteP()
            list_ateP = df_ateP['MODEL'].tolist()
            str_where = ""
            for list in list_ateP:
                str_where += f" OR INSTR(SMT_MS_CODE, '{list}') > 0"
            df_pdbs = self.neuronDb.readDB("SELECT SMT_MS_CODE, SMT_SMT_ASSY, SMT_CRP_GR_NO FROM sap.pdbs0010 WHERE SMT_CRP_GR_NO = '100L1311'" + str_where)
        elif self.className == 'Fam3PowerThread':
            df_pdbs = self.neuronDb.readDB("SELECT SMT_MS_CODE, SMT_SMT_ASSY, SMT_CRP_GR_NO FROM sap.pdbs0010 WHERE SMT_CRP_GR_NO = '100L1313'")
        elif self.className == 'Fam3SpThread':
            df_pdbs = self.neuronDb.readDB("SELECT SMT_MS_CODE, SMT_SMT_ASSY, SMT_CRP_GR_NO FROM sap.pdbs0010 WHERE SMT_CRP_GR_NO = '100L1304' or SMT_CRP_GR_NO = '100L1318' or SMT_CRP_GR_NO = '100L1331' or SMT_CRP_GR_NO = '100L1312' or SMT_CRP_GR_NO = '100L1303'")
        elif self.className == 'JuxtaOldThread':
            df_pdbs = self.neuronDb.readDB("SELECT SMT_MS_CODE, SMT_SMT_ASSY, SMT_CRP_GR_NO FROM sap.pdbs0010 WHERE SMT_CRP_GR_NO = '100L0301' or SMT_CRP_GR_NO = '100L0302' or SMT_CRP_GR_NO = '100L0303' or SMT_CRP_GR_NO = '100L0304' or SMT_CRP_GR_NO = '100L0305' or SMT_CRP_GR_NO = '100L0306' or SMT_CRP_GR_NO = '100L0307'")

        # 불필요한 데이터 삭제
        df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('AST')]
        df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('BMS')]
        df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('WEB')]
        # 사용 Smt Assy를 병렬화
        gb = df_pdbs.groupby('SMT_MS_CODE')
        df_temp = pd.DataFrame([df_pdbs.loc[gb.groups[n], 'SMT_SMT_ASSY'].values for n in gb.groups], index=gb.groups.keys())
        df_temp.columns = ['ROW' + str(i + 1) for i in df_temp.columns]
        df_temp = df_temp.reset_index()
        df_temp.rename(columns={'index': 'SMT_MS_CODE'}, inplace=True)
        df_addSmtAssy = pd.merge(df, df_temp, left_on='MS Code', right_on='SMT_MS_CODE', how='left')
        df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)

        return df_addSmtAssy, dict_smtCnt

    def levelingByCompDate(self, df):
        df['대표모델별_최소착공필요량_per_일'] = 0
        dict_integCnt = {}
        dict_minContCnt = {}
        # 대표모델 별 최소 착공 필요량을 계산
        for i in df.index:
            # 특수모듈의 경우, 대표모델을 특수모듈 조건표의 그룹명으로 설정
            if self.className == 'Fam3MainThread' and str(df['1차_MAX_그룹'][i]) not in ['', '-', 'nan']:
                df['대표모델'][i] = df['1차_MAX_그룹'][i]
            # 각 대표모델 별 적산착공량을 계산하여 딕셔너리에 저장
            if df['대표모델'][i] in dict_integCnt:
                dict_integCnt[df['대표모델'][i]] += int(df['미착공수주잔'][i])
            else:
                dict_integCnt[df['대표모델'][i]] = int(df['미착공수주잔'][i])
            # 이미 완성지정일을 지난경우, 워킹데이 계산을 위해 워킹데이를 1로 설정
            workDay = max(df['남은 워킹데이'][i], 1)
            if df['대표모델'][i] in dict_minContCnt:
                if dict_minContCnt[df['대표모델'][i]][0] < math.ceil(dict_integCnt[df['대표모델'][i]] / workDay):
                    dict_minContCnt[df['대표모델'][i]][0] = math.ceil(dict_integCnt[df['대표모델'][i]] / workDay)
                    dict_minContCnt[df['대표모델'][i]][1] = df['Planned Prod. Completion date'][i]
            else:
                dict_minContCnt[df['대표모델'][i]] = [math.ceil(dict_integCnt[df['대표모델'][i]] / workDay), df['Planned Prod. Completion date'][i]]

            df['대표모델별_최소착공필요량_per_일'][i] = dict_minContCnt[df['대표모델'][i]][0] if self.className == 'Fam3MainThread' and str(df['1차_MAX_그룹'][i]) not in ['', '-', 'nan'] else dict_integCnt[df['대표모델'][i]] / workDay
        df_ori = df.copy()
        if self.className == 'Fam3MainThread':
            # 검사설비능력 최적화 후, 분할된 Row를 다시 합치기 위해 분할 전 데이터를 복사하여 저장
            df_copy = pd.DataFrame(columns=df.columns)
            # 검사설비능력 최적화를 위하여 미착공수주잔을 20대씩 분할 시킴. (ex. 83대 일 경우, Row를 20, 20, 20, 20, 3으로 5분할)
            for i in df.index:
                div, mod = divmod(df['미착공수주잔'][i], 20)
                for j in range(0, div + 1):
                    row = df.iloc[i].copy()
                    row['미착공수주잔'] = 20 if j != div else mod
                    df_copy = df_copy.append(row, ignore_index=True)
            df = df_copy[df_copy['미착공수주잔'] != 0]
        dict_minContCopy = dict_minContCnt.copy()
        # 대표모델 별 최소착공 필요량을 기준으로 평준화 적용 착공량을 계산. 미착공수주잔에서 해당 평준화 적용 착공량을 제외한 수량은 잔여착공량으로 기재
        df['평준화_적용_착공량'] = 0
        for i in df.index:
            if df['긴급오더'][i] == '대상':
                df['평준화_적용_착공량'][i] = int(df['미착공수주잔'][i])
                if df['대표모델'][i] in dict_minContCopy and dict_minContCopy[df['대표모델'][i]][0] >= int(df['미착공수주잔'][i]):
                    dict_minContCopy[df['대표모델'][i]][0] -= int(df['미착공수주잔'][i])
            elif df['대표모델'][i] in dict_minContCopy and dict_minContCopy[df['대표모델'][i]][0] >= int(df['미착공수주잔'][i]):
                df['평준화_적용_착공량'][i] = int(df['미착공수주잔'][i])
                dict_minContCopy[df['대표모델'][i]][0] -= int(df['미착공수주잔'][i])
            else:
                df['평준화_적용_착공량'][i] = dict_minContCopy.get(df['대표모델'][i], [0])[0]
                dict_minContCopy[df['대표모델'][i]] = [max(0, dict_minContCopy.get(df['대표모델'][i], [0])[0] - int(df['미착공수주잔'][i])), df['대표모델'][i]]
        df['잔여_착공량'] = df['미착공수주잔'] - df['평준화_적용_착공량']
        if self.className == 'JuxtaOldThread':
            for i in df.index:
                if df['JOB구분대상'][i] == '대상':
                    if df['Job구분_적용_착공량'][i] > df['평준화_적용_착공량'][i]:
                        df['평준화_적용_착공량'][i] = df['Job구분_적용_착공량'][i]
                        df['잔여_착공량'][i] = df['Job구분_잔여_착공량'][i]
        if self.className == 'Fam3MainThread':
            df = df.sort_values(by=['우선착공', '긴급오더', '당일착공', 'Planned Prod. Completion date', '평준화_적용_착공량'], ascending=[False, False, False, True, False])
        else:
            df = df.sort_values(by=['긴급오더', '당일착공', 'Planned Prod. Completion date', '평준화_적용_착공량'], ascending=[False, False, True, False])
        df = df.reset_index(drop=True)
        return df, df_ori

    def levelingByJobDate(self, df):
        df['대표모델별_Job구분착공필요량_per_일'] = 0
        dict_integCnt = {}
        dict_minContCnt = {}
        # 대표모델 별 최소 착공 필요량을 계산
        for i in df.index:
            if df['JOB구분대상'][i] == '대상':
                # 각 대표모델 별 적산착공량을 계산하여 딕셔너리에 저장
                if df['대표모델'][i] in dict_integCnt:
                    dict_integCnt[df['대표모델'][i]] += int(df['미착공수주잔'][i])
                else:
                    dict_integCnt[df['대표모델'][i]] = int(df['미착공수주잔'][i])
                # 이미 완성지정일을 지난경우, 워킹데이 계산을 위해 워킹데이를 1로 설정
                workDay = max(df['남은JOB구분일'][i], 1)
                if df['대표모델'][i] in dict_minContCnt:
                    if dict_minContCnt[df['대표모델'][i]][0] < math.ceil(dict_integCnt[df['대표모델'][i]] / workDay):
                        dict_minContCnt[df['대표모델'][i]][0] = math.ceil(dict_integCnt[df['대표모델'][i]] / workDay)
                        dict_minContCnt[df['대표모델'][i]][1] = df['Planned Prod. Completion date'][i]
                else:
                    dict_minContCnt[df['대표모델'][i]] = [math.ceil(dict_integCnt[df['대표모델'][i]] / workDay), df['Planned Prod. Completion date'][i]]

                df['대표모델별_Job구분착공필요량_per_일'][i] = dict_minContCnt[df['대표모델'][i]][0] if self.className == 'Fam3MainThread' and str(df['1차_MAX_그룹'][i]) not in ['', '-', 'nan'] else dict_integCnt[df['대표모델'][i]] / workDay
        dict_minContCopy = dict_minContCnt.copy()
        # 대표모델 별 최소착공 필요량을 기준으로 평준화 적용 착공량을 계산. 미착공수주잔에서 해당 평준화 적용 착공량을 제외한 수량은 잔여착공량으로 기재
        df['Job구분_적용_착공량'] = 0
        df['Job구분_잔여_착공량'] = 0
        for i in df.index:
            if df['JOB구분대상'][i] == '대상':
                if df['긴급오더'][i] == '대상':
                    df['Job구분_적용_착공량'][i] = int(df['미착공수주잔'][i])
                    if df['대표모델'][i] in dict_minContCopy and dict_minContCopy[df['대표모델'][i]][0] >= int(df['미착공수주잔'][i]):
                        dict_minContCopy[df['대표모델'][i]][0] -= int(df['미착공수주잔'][i])
                elif df['대표모델'][i] in dict_minContCopy and dict_minContCopy[df['대표모델'][i]][0] >= int(df['미착공수주잔'][i]):
                    df['Job구분_적용_착공량'][i] = int(df['미착공수주잔'][i])
                    dict_minContCopy[df['대표모델'][i]][0] -= int(df['미착공수주잔'][i])
                else:
                    df['Job구분_적용_착공량'][i] = dict_minContCopy.get(df['대표모델'][i], [0])[0]
                    dict_minContCopy[df['대표모델'][i]] = [max(0, dict_minContCopy.get(df['대표모델'][i], [0])[0] - int(df['미착공수주잔'][i])), df['대표모델'][i]]
                df['Job구분_잔여_착공량'][i] = df['미착공수주잔'][i] - df['Job구분_적용_착공량'][i]
        df = df.sort_values(by=['긴급오더', '당일착공', 'Planned Prod. Completion date', 'Job구분_적용_착공량'], ascending=[False, False, True, False])
        df = df.reset_index(drop=True)
        return df

    def levelingBySmtAssy(self, df, dict_smtCnt):
        df['SMT반영_착공량'] = 0
        df['SMT반영_착공량_잔여'] = 0
        # 알람 상세 DataFrame 생성
        df_alarm = pd.DataFrame(columns=["No.", "분류", "L/N", "MS CODE", "SMT ASSY", "수주수량", "부족수량", "검사호기", "대상 검사시간(초)", "필요시간(초)", "완성예정일"])
        alarmNo = 1
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SPThread':
            df_smtUnCheck = pd.read_excel(self.list_masterFile[8])
            list_notManageSmt = df_smtUnCheck['SMT ASSY'].tolist()
            rowNo = len([col for col in df.columns if col.startswith('ROW')]) + 1
            pattern = '|'.join(list_notManageSmt)
            for j in range(1, rowNo):
                df[f'SMT비관리대상{str(j)}'] = df[f'ROW{str(j)}'].str.contains(pattern, case=False)
        elif self.className == 'JuxtaOldThread':
            rowNo = len([col for col in df.columns if col.startswith('ROW')]) + 1
            for j in range(1, rowNo):
                df[f'SMT비관리대상{str(j)}'] = False
                df.loc[df['특주대상'] == '대상', f'SMT비관리대상{str(j)}'] = True

        # 최소착공량에 대해 Smt적용 착공량 계산
        df, dict_smtCnt, alarmNo, df_alarm = self.smtReflectInst(df, False, dict_smtCnt, alarmNo, df_alarm, rowNo)
        df, dict_smtCnt, alarmNo, df_alarm = self.smtReflectInst(df, True, dict_smtCnt, alarmNo, df_alarm, rowNo)
        return df, alarmNo, df_alarm

    def levelingByCapacity(self, df, alarmNo, df_alarm):
        today = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            today = self.date
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SpThread':
            df_limitCtCond = pd.read_excel(self.list_masterFile[13])
        if self.className == 'Fam3MainThread':
            fam3ProductDb = self.loadFam3ProductDB()
            df_productTime = fam3ProductDb.readDB('SELECT * FROM FAM3_PRODUCT_TIME_TB')
            # 전체 검사시간을 계산
            df_productTime['TotalTime'] = (df_productTime['M_FUNCTION_CHECK'].apply(self.getSec) + df_productTime['A_FUNCTION_CHECK'].apply(self.getSec))
            df_productTime['대표모델'] = df_productTime['MODEL'].str[:9]
            df_productTime = df_productTime.drop_duplicates(['대표모델'])
            df_productTime = df_productTime.reset_index(drop=True)
            df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].apply(self.delBackslash)
            df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].str.strip()
            df = pd.merge(df, df_productTime[['대표모델', 'TotalTime', 'INSPECTION_EQUIPMENT']], on='대표모델', how='left')
            df = df[~df['INSPECTION_EQUIPMENT'].str.contains('None')]
            df = df.assign(임시수량=0, 설비능력반영_착공량=0, 임시수량_잔여=0, 설비능력반영_착공량_잔여=0, 남은착공량=0).reset_index()
            dict_ate = {row['검사호기 분류']: (60 * row['가동시간']) * 60 for i, row in pd.read_excel(self.list_masterFile[12]).iterrows()}
            list_priorityAte = [str(row['검사호기 분류']) for i, row in pd.read_excel(self.list_masterFile[12]).iterrows() if str(row['설비능력 MAX 사용']) not in ['', 'nan']]
            # 검사설비 우선사용 계산을 위해 데이터프레임 신규 선언
            df_priority, df_unPriority = pd.DataFrame(columns=df.columns), pd.DataFrame(columns=df.columns)
            # 우선사용 검사설비 대상을 찾아 우선사용/비사용 데이터 프레임으로 분할
            if len(list_priorityAte) > 0:
                for ate in list_priorityAte:
                    df_priority = pd.concat([df_priority, df[df['INSPECTION_EQUIPMENT'].str.contains(ate)]])
                    df_priority = pd.concat([df_priority, df[df['긴급오더'].notnull()]])
                    df_priority = pd.concat([df_priority, df[df['당일착공'].str.contains('대상')]])
                    df_unPriority = df[~df['INSPECTION_EQUIPMENT'].str.contains(ate)]
                    df_unPriority = df_unPriority[df_unPriority['긴급오더'].isnull()]
                    df_unPriority = df_unPriority[~df_unPriority['당일착공'].str.contains('대상')]
                    df_unPriority = df_unPriority.drop_duplicates(['index'])
                    df_priority = df_priority.drop_duplicates(['index'])
            else:
                df_unPriority = df.copy()
            limitCtCnt = df_limitCtCond[df_limitCtCond['상세구분'] == 'MAIN']['허용수량'].values[0]
            # 설비능력 반영 착공량 계산
            for df_inst in [df_priority, df_unPriority]:
                df_inst, dict_ate, alarmNo, df_alarm, self.firstModCnt, limitCtCnt = self.fam3MainCapaReflect(df_inst, False, dict_ate, df_alarm, alarmNo, self.firstModCnt, limitCtCnt)
                # 잔여 착공량에 대해 설비능력 반영 착공량 계산
                df_inst, dict_ate, alarmNo, df_alarm, self.firstModCnt, limitCtCnt = self.fam3MainCapaReflect(df_inst, True, dict_ate, df_alarm, alarmNo, self.firstModCnt, limitCtCnt)
            # 검사설비 우선사용/비사용 데이터프레임을 다시 통합.
            df = pd.concat([df_priority, df_unPriority])
            # 잔여 검사설비능력 출력을 위하여 데이터프레임 선언
            df_dict = pd.DataFrame(data=dict_ate, index=[0]).T.reset_index().rename(columns={'index': '검사설비', 0: '남은 시간'})[['검사설비', '남은 시간']]
            df_dict['남은 시간'] = df_dict['남은 시간'].apply(self.convertSecToTime)
            df_dict = pd.merge(df_dict, pd.read_excel(self.list_masterFile[12]), how='left', left_on='검사설비', right_on='검사호기 분류')
            df_dict = df_dict[['검사설비', '가동시간', '남은 시간']]
            # 현재 시간의 잔여검사설비 출력
            if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}'):
                os.makedirs(f'.\\Result\\FAM3\\{str(today)}')
            if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}'):
                os.makedirs(f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}')
            now = time.strftime('%H%M%S')
            df_dict.to_excel(f'.\\Result\\FAM3\\{str(today)}\\{str(self.cb_round)}\\잔여검사설비능력_{now}.xlsx')
            df = df.reset_index(drop=True)
        elif self.className == 'Fam3PowerThread':
            # maxCnt = self.firstModCnt + int(self.df_etcOrderInput['착공량'][0])
            maxCnt = self.firstModCnt
            df = df.assign(Linkage_Number=df['Linkage Number'].astype(str), MODEL=df['MS Code'].str[:6])
            # 전원 조건표 불러오기
            df_powerCond = pd.read_excel(self.list_masterFile[6])
            df_powerCond['상세구분'] = df_powerCond['상세구분'].fillna(method='ffill')
            df_powerCond['최대허용비율'] = df_powerCond['최대허용비율'].fillna(method='ffill')
            df_mergeCond = pd.merge(df, df_powerCond, on='MODEL', how='left')
            df_mergeCond['MAX대수'] = df_mergeCond['MAX대수'].fillna('-')
            df_mergeCond['공수'] = df_mergeCond['공수'].fillna(1)
            df_mergeCond = df_mergeCond.sort_values(by=['우선착공'], ascending=[False])
            df_mergeCond = df_mergeCond.reset_index(drop=True)
            dict_ratio, dict_alarmRatio, dict_max, dict_alarmMax = {}, {}, {}, {}
            df_mergeCond['설비능력반영_착공공수'] = 0
            df_mergeCond['설비능력반영_착공공수_잔여'] = 0
            df_mergeCond['설비능력반영_착공량'] = 0
            df_mergeCond['설비능력반영_착공량_잔여'] = 0
            # 조건표에 있는 내용을 각 LinkageNo에 적용
            for i in df_powerCond.index:
                dict_ratio[str(df_powerCond['상세구분'][i])] = round(float(df_powerCond['최대허용비율'][i]) * self.firstModCnt)
                dict_alarmRatio[str(df_powerCond['상세구분'][i])] = round(float(df_powerCond['최대허용비율'][i]) * maxCnt)
                if str(df_powerCond['MAX대수'][i]) != '' and str(df_powerCond['MAX대수'][i]) != '-':
                    dict_max[str(df_powerCond['MODEL'][i])] = round(float(df_powerCond['최대허용비율'][i]) * self.firstModCnt * float(df_powerCond['MAX대수'][i]))
                    dict_alarmMax[str(df_powerCond['MODEL'][i])] = round(float(df_powerCond['최대허용비율'][i]) * maxCnt * float(df_powerCond['MAX대수'][i]))
                else:
                    dict_max[str(df_powerCond['MODEL'][i])] = self.firstModCnt
                    dict_alarmMax[str(df_powerCond['MODEL'][i])] = self.firstModCnt
            limitCtCnt = df_limitCtCond[df_limitCtCond['상세구분'] == 'POWER']['허용수량'].values[0]
            # 비율제한 적용 (최소필요착공량)
            df_mergeCond, dict_ratio, dict_max, alarmNo, df_alarm, limitCtCnt, dict_alarmRatio, dict_alarmMax, self.firstModCnt = self.fam3PowerCapaReflect(df_mergeCond,
                                                                                                                                                    False,
                                                                                                                                                    dict_ratio,
                                                                                                                                                    dict_max,
                                                                                                                                                    alarmNo,
                                                                                                                                                    df_alarm,
                                                                                                                                                    limitCtCnt,
                                                                                                                                                    dict_alarmRatio,
                                                                                                                                                    dict_alarmMax,
                                                                                                                                                    self.firstModCnt)
            # 비율제한 적용 (여유분)
            df_mergeCond, dict_ratio, dict_max, alarmNo, df_alarm, limitCtCnt, dict_alarmRatio, dict_alarmMax, self.firstModCnt = self.fam3PowerCapaReflect(df_mergeCond,
                                                                                                                                                    True,
                                                                                                                                                    dict_ratio,
                                                                                                                                                    dict_max,
                                                                                                                                                    alarmNo,
                                                                                                                                                    df_alarm,
                                                                                                                                                    limitCtCnt,
                                                                                                                                                    dict_alarmRatio,
                                                                                                                                                    dict_alarmMax,
                                                                                                                                                    self.firstModCnt)
            df = df_mergeCond.copy()
        elif self.className == 'Fam3SpThread':
            df_cond = pd.read_excel(self.list_masterFile[7])
            df_cond['No'] = df_cond['No'].fillna(method='ffill')
            df_cond['1차_MAX_그룹'] = df_cond['1차_MAX_그룹'].fillna(method='ffill')
            df_cond['2차_MAX_그룹'] = df_cond['2차_MAX_그룹'].fillna(method='ffill')
            df_cond['1차_MAX'] = df_cond['1차_MAX'].fillna(method='ffill')
            df_cond['2차_MAX'] = df_cond['2차_MAX'].fillna(method='ffill')
            df['MODEL'] = df['MS Code'].str[:7]
            df['MODEL'] = df['MODEL'].astype(str).apply(self._delHypen)
            df = pd.merge(df, df_cond, on='MODEL', how='left')
            df['1차_MAX_그룹'] = df['1차_MAX_그룹'].fillna('-')
            df['2차_MAX_그룹'] = df['2차_MAX_그룹'].fillna('-')
            df['공수'] = df['공수'].fillna(1)
            dict_firstGr = {}
            dict_secGr = {}
            dict_category = {'모듈': self.firstModCnt, '비모듈': self.secondModCnt}
            # 특수 조건표 불러오기
            for i in df_cond.index:
                if str(df_cond['2차_MAX_그룹'][i]) not in ['', 'nan', '-']:
                    dict_firstGr[df_cond['1차_MAX_그룹'][i]] = df_cond['1차_MAX'][i]
                    dict_secGr[df_cond['2차_MAX_그룹'][i]] = df_cond['2차_MAX'][i]
                elif str(df_cond['1차_MAX_그룹'][i]) not in ['', 'nan', '-']:
                    dict_firstGr[df_cond['1차_MAX_그룹'][i]] = df_cond['1차_MAX'][i]
            # df = df[df['검사호기'] != 'P']
            # 메인 클래스로부터 전달받은 P호기 검사대상을 가져와서 병합처리 (중복되었으나 불필요한 컬럼은 삭제)
            # if len(self.df_receiveMain) > 0:
            #     df_receiveMain = self.df_receiveMain
            #     df_receiveMain['MODEL'] = df_receiveMain['MS Code'].str[:6]
            #     df_receiveMain['공수'] = 1
            #     df_receiveMain['모듈 구분'] = '모듈'
            #     df = pd.concat([df, df_receiveMain])
            #     del_cols = ['구분', 'No', '상세구분', '검사호기', '1차_MAX_그룹', '2차_MAX_그룹', '1차_MAX', '2차_MAX', '공수', '우선착공']
            #     df = df.drop(columns=del_cols)
            #     df = pd.merge(df, df_cond, on='MODEL', how='left')
            #     df['1차_MAX_그룹'] = df['1차_MAX_그룹'].fillna('-')
            #     df['2차_MAX_그룹'] = df['2차_MAX_그룹'].fillna('-')
            #     df['공수'] = df['공수'].fillna(1)
            df = df.sort_values(by=['긴급오더', '우선착공', 'Planned Prod. Completion date'], ascending=[False, False, True])
            df = df.reset_index(drop=True)
            limitCtCnt = df_limitCtCond[df_limitCtCond['상세구분'] == 'OTHER']['허용수량'].values[0]
            df['설비능력반영_착공량'] = 0
            # 조건표의 제한대수를 적용하여 착공 (최소필요착공량)
            df, dict_category, dict_firstGr, dict_secGr, alarmNo, df_alarm, limitCtCnt = self.fam3SPCapaReflect(df,
                                                                                                            False,
                                                                                                            dict_category,
                                                                                                            dict_firstGr,
                                                                                                            dict_secGr,
                                                                                                            alarmNo,
                                                                                                            df_alarm,
                                                                                                            limitCtCnt)
            df['설비능력반영_착공량_잔여'] = 0
            # 조건표의 제한대수를 적용하여 착공 (여유분)
            df, dict_category, dict_firstGr, dict_secGr, alarmNo, df_alarm, limitCtCnt = self.fam3SPCapaReflect(df,
                                                                                                            True,
                                                                                                            dict_category,
                                                                                                            dict_firstGr,
                                                                                                            dict_secGr,
                                                                                                            alarmNo,
                                                                                                            df_alarm,
                                                                                                            limitCtCnt)
        elif self.className == 'JuxtaOldThread':
            df_grCond = pd.read_excel(self.list_masterFile[3], sheet_name=0)
            df_grCond = df_grCond.fillna(method='ffill')
            df_modelCond = pd.read_excel(self.list_masterFile[3], sheet_name=1)
            df_modelCond = df_modelCond.fillna(method='ffill')
            df_modelCond['No'] = df_modelCond['No'].astype(str)
            df_ateCond = pd.read_excel(self.list_masterFile[3], sheet_name=2)
            # 쉼표를 기준으로 [검사 시설] 열을 분할하고 새 열 생성
            df_split = df_modelCond['대상 검사설비'].str.replace(' ', '').str.split(',', expand=True)

            # 필요한 최대 열 수 결정
            colsCnt = df_split.shape[1]

            # 최대 열 수에 따라 새 열 이름 만들기
            colNames = ['대상 검사설비 {}'.format(i) for i in range(1, colsCnt + 1)]

            # 분할된 데이터 프레임에 새 열 이름을 할당
            df_split.columns = colNames

            # 분할된 데이터 프레임을 원래 데이터 프레임과 연결
            df_modelCond = pd.concat([df_modelCond, df_split], axis=1)

            df_grCondCopy = df_grCond.copy()
            df_grCondCopy = df_grCondCopy.drop_duplicates(['착공비율 그룹명'])
            dict_gr = {}
            for i in df_grCondCopy.index:
                dict_gr[df_grCondCopy['착공비율 그룹명'][i]] = math.ceil((float(self.firstModCnt) + float(self.secondModCnt)) * df_grCondCopy['최대 착공비율(%)'][i])

            dict_model = {}
            dict_modelGr = {}
            for i in df_modelCond.index:
                if df_modelCond['대표모델'][i] == '특주':
                    dict_modelGr['특주'] = df_modelCond['일 최대 착공량'][i]
                elif df_modelCond['일 최대 착공량 그룹'][i] in '-' and df_modelCond['일 최대 착공량'][i] not in ['', '-', 'nan']:
                    dict_model[df_modelCond['No'][i]] = df_modelCond['일 최대 착공량'][i]

            df_modelCondCopy = df_modelCond.copy()
            df_modelCondCopy = df_modelCondCopy.drop_duplicates(['일 최대 착공량 그룹'])
            for i in df_modelCondCopy.index:
                if df_modelCondCopy['일 최대 착공량 그룹'][i] not in ['', '-', 'nan']:
                    dict_modelGr[df_modelCondCopy['일 최대 착공량 그룹'][i]] = df_modelCondCopy['일 최대 착공량'][i]

            dict_ate = {}
            for i in df_ateCond.index:
                dict_ate[df_ateCond['검사설비'][i]] = df_ateCond['일 생산가능량'][i]

            df = pd.merge(df, df_grCond[['대상 MSCODE', '착공비율 그룹명']], left_on='대표모델', right_on='대상 MSCODE', how='left')
            df['사양번호'] = ''

            for i in df.index:
                for j in df_modelCond.index:
                    if df_modelCond['대표모델'][j] != '-':
                        if df['대표모델'][i] == df_modelCond['대표모델'][j]:
                            if df_modelCond['부가사양'][j] == '-':
                                if str(df['사양번호'][i]) in ['', 'nan']:
                                    df['사양번호'][i] = str(df_modelCond['No'][j])
                                else:
                                    df['사양번호'][i] += ',' + str(df_modelCond['No'][j])
                            else:
                                if df['MS Code'][i] in df_modelCond['부가사양'][j]:
                                    if str(df['사양번호'][i]) in ['', 'nan']:
                                        df['사양번호'][i] = str(df_modelCond['No'][j])
                                    else:
                                        df['사양번호'][i] += ',' + str(df_modelCond['No'][j])
                    else:
                        if df_modelCond['부가사양'][j] != '-':
                            if df_modelCond['부가사양'][j] == '특주' and df['특주대상'][i] == '대상':
                                if str(df['사양번호'][i]) in ['', 'nan']:
                                    df['사양번호'][i] = str(df_modelCond['No'][j])
                                else:
                                    df['사양번호'][i] += ',' + str(df_modelCond['No'][j])
                            elif df_modelCond['부가사양'][j] in df['MS Code'][i]:
                                if str(df['사양번호'][i]) in ['', 'nan']:
                                    df['사양번호'][i] = str(df_modelCond['No'][j])
                                else:
                                    df['사양번호'][i] += ',' + str(df_modelCond['No'][j])

            df_split = df['사양번호'].str.replace(' ', '').str.split(',', expand=True)
            colsCnt = df_split.shape[1]

            colNames = ['사양번호 {}'.format(i) for i in range(1, colsCnt + 1)]

            df_split.columns = colNames

            df = pd.concat([df, df_split], axis=1)

            df['설비능력반영_착공량'] = 0
            df['설비능력반영_착공량_잔여'] = 0
            manualInspCnt = 20
            maxCnt = self.firstModCnt + self.secondModCnt
            df, dict_gr, dict_model, dict_modelGr, dict_ate, manualInspCnt, maxCnt, alarmNo, df_alarm = self.juxtaCapaReflect(df, df_modelCond, False, dict_gr, dict_model, dict_modelGr, dict_ate, manualInspCnt, maxCnt, alarmNo, df_alarm)
            df, dict_gr, dict_model, dict_modelGr, dict_ate, manualInspCnt, maxCnt, alarmNo, df_alarm = self.juxtaCapaReflect(df, df_modelCond, True, dict_gr, dict_model, dict_modelGr, dict_ate, manualInspCnt, maxCnt, alarmNo, df_alarm)
        return df, alarmNo, df_alarm

