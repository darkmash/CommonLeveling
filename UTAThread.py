import datetime
import os
import re
import math
import numpy as np
from PyQt5.QtCore import pyqtSignal, QObject
import pandas as pd
import cx_Oracle
from pathlib import Path
import debugpy
import time
from configparser import ConfigParser
from WorkDayCalculator import WorkDayCalculator


class UTAThread(QObject):
    # 클래스 외부에서 사용할 수 있도록 시그널 선언
    returnError = pyqtSignal(Exception)
    returnInfo = pyqtSignal(str)
    returnWarning = pyqtSignal(str)
    returnEnd = pyqtSignal(bool)
    returnPb = pyqtSignal(int)
    returnMaxPb = pyqtSignal(int)

    # 초기화
    def __init__(self, debugFlag, date, constDate, list_masterFile, moduleMaxCnt, emgHoldList):
        super().__init__(),
        self.isDebug = debugFlag
        self.date = date
        self.constDate = constDate
        self.list_masterFile = list_masterFile
        self.moduleMaxCnt = moduleMaxCnt
        self.emgHoldList = emgHoldList

    # 콤마 삭제용 내부함수
    def delComma(self, value):
        return str(value).split('.')[0]

    # 디비 불러오기 공통내부함수
    def readDB(self, ip, port, sid, userName, password, sql):
        # 오라클 클라이언트 폴더 지정 (프로그램 파일에 이미 넣어둠)
        location = r'.\\instantclient_21_7'
        # 해당 폴더를 환경변수에 지정
        os.environ["PATH"] = location + ";" + os.environ["PATH"]
        # 입력된 Dsn 설정 및 접속정보로 오라클 접속
        dsn = cx_Oracle.makedsn(ip, port, sid)
        db = cx_Oracle.connect(userName, password, dsn)
        cursor = db.cursor()
        # Sql 실행
        cursor.execute(sql)
        out_data = cursor.fetchall()
        # 결과값을 Dataframe화
        df_oracle = pd.DataFrame(out_data)
        # 컬럼명 지정
        col_names = [row[0] for row in cursor.description]
        df_oracle.columns = col_names
        return df_oracle

    # 생산시간 합계용 내부함수
    def getSec(self, time_str):
        time_str = re.sub(r'[^0-9:]', '', str(time_str))
        if len(time_str) > 0:
            h, m, s = time_str.split(':')
            return int(h) * 3600 + int(m) * 60 + int(s)
        else:
            return 0

    # 백슬래쉬 삭제용 내부함수
    def delBackslash(self, value):
        value = re.sub(r"\\c", "", str(value))
        return value

    # 초단위의 시간을 시/분/초로 분할
    def convertSecToTime(self, seconds):
        seconds = seconds % (24 * 3600)
        hour = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60
        return "%d:%02d:%02d" % (hour, minutes, seconds)

    # 알람 상세 누적 기록용 내부함수
    def concatAlarmDetail(self, df_target, no, category, df_data, index, smtAssy, shortageCnt):
        """
        Args:
            df_target(DataFrame)    : 알람상세내역 DataFrame
            no(int)                 : 알람 번호
            category(str)           : 알람 분류
            df_data(DataFrame)      : 원본 DataFrame
            index(int)              : 원본 DataFrame의 인덱스
            smtAssy(str)            : Smt Assy 이름
            shortageCnt(int)        : 부족 수량
        Return:
            return(DataFrame)       : 알람상세 Merge결과 DataFrame
        """
        df_result = pd.DataFrame()
        if category == '1':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '2':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '-',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": df_data['INSPECTION_EQUIPMENT'][index],
                                                                "대상 검사시간(초)": df_data['TotalTime'][index],
                                                                "필요시간(초)": shortageCnt * df_data['TotalTime'][index],
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타1':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '미등록',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": 0,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타2':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": '-',
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타3':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타4':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": smtAssy,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        return [df_result, no + 1]

    # SMT Assy 반영 착공로직
    def smtReflectInst(self, df_input, isRemain, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_smtCnt(Dict)           : Smt잔여량 Dict
            alarmDetailNo(int)          : 알람 번호
            df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame
            rowNo(int)                  : 사용 Smt Assy 갯수
        Return:
            return(List)
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_smtCnt(Dict)           : Smt잔여량 Dict (갱신 후)
                alarmDetailNo(int)          : 알람 번호
                df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame (갱신 후)
        """
        instCol = '평준화_적용_착공량'
        resultCol = 'SMT반영_착공량'
        if isRemain:
            instCol = '잔여_착공량'
            resultCol = 'SMT반영_착공량_잔여'
        # 행별로 확인
        for i in df_input.index:
            # 사용 Smt Assy 개수 확인
            for j in range(1, rowNo):
                if j == 1:
                    rowCnt = 1
                if (str(df_input[f'ROW{str(j)}'][i]) != '' and str(df_input[f'ROW{str(j)}'][i]) != 'nan'):
                    rowCnt = j
                else:
                    break
            if rowNo == 1:
                rowCnt = 1
            minCnt = 9999
            # 각 SmtAssy 별로 착공 가능 대수 확인
            for j in range(1, rowCnt + 1):
                smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                if (df_input['SMT_MS_CODE'][i] != 'nan' and df_input['SMT_MS_CODE'][i] != 'None' and df_input['SMT_MS_CODE'][i] != ''):
                    if (smtAssyName != '' and smtAssyName != 'nan' and smtAssyName != 'None'):
                        # 긴급오더 혹은 당일착공 대상일 경우, SMT Assy 잔량에 관계없이 착공 실시.
                        # SMT Assy가 부족할 경우에는 분류1 알람을 발생.
                        if df_input['긴급오더'][i] == '대상' or df_input['당일착공'][i] == '대상':
                            # MS Code와 연결된 SMT Assy가 있을 경우, 정상적으로 로직을 실행
                            if smtAssyName in dict_smtCnt:
                                if dict_smtCnt[smtAssyName] < 0:
                                    diffCnt = df_input['미착공수주잔'][i]
                                    if dict_smtCnt[smtAssyName] + df_input['미착공수주잔'][i] > 0:
                                        diffCnt = 0 - dict_smtCnt[smtAssyName]
                                    if not isRemain:
                                        if dict_smtCnt[smtAssyName] > 0:
                                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '1', df_input, i, smtAssyName, diffCnt)
                            # SMT Assy가 DB에 등록되지 않은 경우, 기타3 알람을 출력.
                            else:
                                minCnt = 0
                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타3', df_input, i, smtAssyName, 0)
                        # 긴급오더 혹은 당일착공 대상이 아닐 경우, SMT Assy 잔량을 확인 후, SMT Assy 잔량이 부족할 경우, 부족한 양만큼 착공.
                        else:
                            # 사용하는 SmtAssy가 이미 등록된 SmtAssy일 경우의 로직
                            if smtAssyName in dict_smtCnt:
                                # 최소필요착공량보다 SmtAssy 수량이 여유 있는 경우, 그대로 착공
                                if dict_smtCnt[smtAssyName] >= df_input[instCol][i]:
                                    # 사용하는 SmtAssy가 다수 일 경우를 고려하여 최소수량 확인
                                    if minCnt > df_input[instCol][i]:
                                        minCnt = df_input[instCol][i]
                                # SmtAssy 수량의 여유가 없는 경우
                                else:
                                    # 최소수량과 SmtAssy수량을 다시 비교
                                    if dict_smtCnt[smtAssyName] > 0:
                                        if minCnt > dict_smtCnt[smtAssyName]:
                                            minCnt = dict_smtCnt[smtAssyName]
                                    # SmtAssy수량이 0개 인 경우, 최소수량을 0으로 전환
                                    else:
                                        minCnt = 0
                                    # 최소착공필요량 전체에 비해 SmtAssy수량이 부족한 경우, 알람을 출력.
                                    if not isRemain:
                                        if dict_smtCnt[smtAssyName] > 0:
                                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '1', df_input, i, smtAssyName, df_input[instCol][i] - dict_smtCnt[smtAssyName])
                            # SMT Assy가 DB에 등록되지 않은 경우, 기타3 알람을 출력.
                            else:
                                minCnt = 0
                                df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타3', df_input, i, smtAssyName, 0)
                # MS Code와 연결된 SMT Assy가 등록되지 않았을 경우, 기타1 알람을 출력.
                else:
                    minCnt = 0
                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타1', df_input, i, '미등록', 0)
            # 최소 수량을 1번이라도 갱신한 경우, 결과컬럼의 값을 minCnt로 대체
            if minCnt != 9999:
                df_input[resultCol][i] = minCnt
            # 갱신하지 않았을 경우, 기존 입력값을 그대로 출력
            else:
                df_input[resultCol][i] = df_input[instCol][i]
            # 사용되는 각 Smt Assy 수량에서 결과값을 빼기위한 로직
            for j in range(1, rowCnt + 1):
                if (smtAssyName != '' and smtAssyName != 'nan' and smtAssyName != 'None'):
                    smtAssyName = str(df_input[f'ROW{str(j)}'][i])
                    dict_smtCnt[smtAssyName] -= df_input[resultCol][i]
        return [df_input, dict_smtCnt, alarmDetailNo, df_alarmDetail]

    # 검사설비 반영 착공로직
    def ateReflectInst(self, df_input, isRemain, dict_ate, df_alarmDetail, alarmDetailNo, moduleMaxCnt, limitCtCnt):
        """
        Args:
            df_input(DataFrame)         : 입력 DataFrame
            isRemain(Bool)              : 잔여착공 여부 Flag
            dict_ate(Dict)              : 잔여 검사설비능력 Dict
            alarmDetailNo(int)          : 알람 번호
            df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame
            moduleMaxCnt(int)           : 최대착공량
            limitCtCnt(int)             : CT제한 착공량
        Return:
            return(List)
                df_input(DataFrame)         : 입력 DataFrame (갱신 후)
                dict_ate(Dict)              : 잔여 검사설비능력 Dict (갱신 후)
                alarmDetailNo(int)          : 알람 번호
                df_alarmDetail(DataFrame)   : 알람 상세 기록용 DataFrame (갱신 후)
                moduleMaxCnt(int)           : 최대착공량 (갱신 후)
                limitCtCnt(int)             : CT제한 착공량 (갱신 후)
        """
        # 여유 착공량인지 확인 후, 컬럼명을 지정
        if isRemain:
            smtReflectCnt = 'SMT반영_착공량_잔여'
            tempAteCnt = '임시수량_잔여'
            ateReflectCnt = '설비능력반영_착공량_잔여'
        else:
            smtReflectCnt = 'SMT반영_착공량'
            tempAteCnt = '임시수량'
            ateReflectCnt = '설비능력반영_착공량'
        for i in df_input.index:
            # 디버그로 남은착공량의 과정을 보기 위해 데이터프레임에 기록
            df_input['남은착공량'][i] = moduleMaxCnt
            # 검사시간이 있는 모델만 적용
            if (str(df_input['TotalTime'][i]) != '') and (str(df_input['TotalTime'][i]) != 'nan'):
                # 검사설비가 있는 모델만 적용
                if (str(df_input['INSPECTION_EQUIPMENT'][i]) != '') and (str(df_input['INSPECTION_EQUIPMENT'][i]) != 'nan'):
                    # 임시 검사시간과 검사설비를 가지고 있는 변수 선언
                    tempTime = 0
                    ateName = ''
                    # 긴급오더 or 당일착공 대상은 검사설비 능력이 부족하여도 강제 착공. 그리고 알람을 기록
                    if (str(df_input['긴급오더'][i]) == '대상') or (str(df_input['당일착공'][i]) == '대상'):
                        # 대상 검사설비를 한개씩 분할
                        ateList = list(df_input['INSPECTION_EQUIPMENT'][i])
                        dict_temp = {}
                        # 입력되어진 검사설비 시간을 임시 검사설비 딕셔너리에 넣음.
                        for ate in ateList:
                            dict_temp[ate] = dict_ate[ate]
                        # 여유있는 검사설비 순으로 정렬
                        dict_temp = sorted(dict_temp.items(), key=lambda item: item[1], reverse=True)
                        # 여유있는 검사설비 순으로 검시시간을 계산하여 넣는다.
                        for ate in dict_temp:
                            # 최대 착공량이 0 초과일 경우에만 해당 로직을 실행.
                            if moduleMaxCnt > 0:
                                # 일단 가장 여유있는 검사설비와 그 시간을 임시로 가져옴
                                tempTime = dict_ate[ate[0]]
                                ateName = ate[0]
                                # if ate[0] == df_input['INSPECTION_EQUIPMENT'][i][0]:
                                # 임시 수량 컬럼에 Smt 반영 착공량을 입력
                                df_input[tempAteCnt][i] = df_input[smtReflectCnt][i]
                                if df_input[tempAteCnt][i] != 0:
                                    # 해당 검사설비능력에서 착공분만큼 삭감
                                    dict_ate[ateName] -= df_input['TotalTime'][i] * df_input[tempAteCnt][i]
                                    df_input[ateReflectCnt][i] += df_input[tempAteCnt][i]
                                    # 특수모듈인 경우에는 전체 착공랴에서 빼지 않음
                                    if df_input['특수대상'][i] != '대상':
                                        moduleMaxCnt -= df_input[tempAteCnt][i]
                                    # 임시수량은 초기화
                                    df_input[tempAteCnt][i] = 0
                                    # CT사양의 경우 별도 CT제한수량에서도 삭감
                                    if '/CT' in df_input['MS Code'][i]:
                                        limitCtCnt -= df_input[tempAteCnt][i]
                                    break
                                else:
                                    break
                        # 최대착공량이 0 미만일 경우, 알람 출력
                        if moduleMaxCnt < 0:
                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타2', df_input, i, '-', 0)
                        # CT제한대수가 0 미만일 경우, 알람 출력
                        if limitCtCnt < 0:
                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타4', df_input, i, '-', 0)
                            # break
                        # 검사설비능력이 0미만일 경우, 알람 출력
                        if ateName != '' and dict_ate[ateName] < 0:
                            df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '2', df_input, i, '-', math.floor((0 - dict_ate[ateName]) / df_input['TotalTime'][i]))
                            dict_ate[ateName] = 0
                        # 긴급오더 or 당일착공이 아닌 경우는 검사설비 능력을 반영하여 착공 실시
                    else:
                        # 긴급오더 처리 시, 0미만으로 떨어진 각 카운터를 0으로 초기화
                        if moduleMaxCnt < 0:
                            moduleMaxCnt = 0
                        if limitCtCnt < 0:
                            limitCtCnt = 0
                        # 첫 착공인지 확인하는 플래그 선언
                        isFirst = True
                        ateList = list(df_input['INSPECTION_EQUIPMENT'][i])
                        dict_temp = {}
                        for ate in ateList:
                            dict_temp[ate] = dict_ate[ate]
                        dict_temp = sorted(dict_temp.items(), key=lambda item: item[1], reverse=True)
                        for ate in dict_temp:
                            if tempTime <= dict_ate[ate[0]]:
                                tempTime = dict_ate[ate[0]]
                                ateName = ate[0]
                                # if ate[0] == df_input['INSPECTION_EQUIPMENT'][i][0]:
                                # 비교리스트에 [Smt반영 착공량], [최대착공량]을 입력
                                compareList = [df_input[smtReflectCnt][i], moduleMaxCnt]
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
                                            moduleMaxCnt -= df_input[tempAteCnt][i]
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
                                                if moduleMaxCnt >= j:
                                                    df_input[ateReflectCnt][i] = int(df_input[ateReflectCnt][i]) + j
                                                    dict_ate[ateName] -= int(df_input['TotalTime'][i]) * j
                                                    df_input[tempAteCnt][i] = tempCnt - j
                                                    if df_input['특수대상'][i] != '대상':
                                                        moduleMaxCnt -= j
                                                    if '/CT' in df_input['MS Code'][i]:
                                                        limitCtCnt -= j
                                                    isFirst = False
                                                    break
                                # else:
                                #     break
            # CT사양의 최소필요착공량을 착공 못할 경우, 알람을 발생 시킴
            if not isRemain and (df_input[smtReflectCnt][i] > df_input[ateReflectCnt][i]):
                if '/CT' in df_input['MS Code'][i] and limitCtCnt == 0 and df_input[smtReflectCnt][i] > 0:
                    df_alarmDetail, alarmDetailNo = self.concatAlarmDetail(df_alarmDetail, alarmDetailNo, '기타4', df_input, i, '-', df_input[smtReflectCnt][i] - df_input[ateReflectCnt][i])

        return [df_input, dict_ate, alarmDetailNo, df_alarmDetail, moduleMaxCnt, limitCtCnt]

    def run(self):
        # pandas 경고없애기 옵션 적용
        pd.set_option('mode.chained_assignment', None)
        try:
            start = time.time()
            # 쓰레드 디버깅을 위한 처리
            if self.isDebug:
                debugpy.debug_this_thread()
            # 프로그레스바의 최대값 설정
            maxPb = 210
            self.returnMaxPb.emit(maxPb)
            # 최대착공량 0 초과 입력할 경우에만 로직 실행
            if self.moduleMaxCnt > 0:
                progress = 0
                self.returnPb.emit(progress)
                # 긴급오더, 홀딩오더 불러오기
                emgLinkage = self.emgHoldList[0]
                emgmscode = self.emgHoldList[1]
                holdLinkage = self.emgHoldList[2]
                holdmscode = self.emgHoldList[3]
                # 긴급오더, 홀딩오더 데이터프레임화
                df_emgLinkage = pd.DataFrame({'Linkage Number': emgLinkage})
                df_emgmscode = pd.DataFrame({'MS Code': emgmscode})
                df_holdLinkage = pd.DataFrame({'Linkage Number': holdLinkage})
                df_holdmscode = pd.DataFrame({'MS Code': holdmscode})
                # 각 Linkage Number 컬럼의 타입을 일치시킴
                df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(np.int64)
                df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(np.int64)
                # 긴급오더, 홍딩오더 Join 전 컬럼 추가
                df_emgLinkage['긴급오더'] = '대상'
                df_emgmscode['긴급오더'] = '대상'
                df_holdLinkage['홀딩오더'] = '대상'
                df_holdmscode['홀딩오더'] = '대상'
                # 레벨링 리스트 불러오기(멀티프로세싱 적용 후, 분리 예정)
                df_levelingMain = pd.read_excel(self.list_masterFile[1])
                # 레벨링 리스트의 착공 당일의 마지막 No를 가져오기 위한 처리
                df_constDate = df_levelingMain[df_levelingMain['Scheduled Start Date (*)'] == self.constDate]
                df_constDate = df_constDate[df_constDate['Sequence No'].notnull()]
                if len(df_constDate) > 0:
                    df_constDate = df_constDate[df_constDate['Sequence No'].str.contains('D0')]
                if len(df_constDate) > 0:
                    maxNo = df_constDate['No (*)'].max()
                else:
                    maxNo = 0
                # 미착공 대상만 추출(Main)
                df_levelingMainDropSeq = df_levelingMain[df_levelingMain['Sequence No'].isnull()]
                df_levelingMainUndepSeq = df_levelingMain[df_levelingMain['Sequence No'] == 'Undep']
                df_levelingMainUncorSeq = df_levelingMain[df_levelingMain['Sequence No'] == 'Uncor']
                df_levelingMain = pd.concat([df_levelingMainDropSeq, df_levelingMainUndepSeq, df_levelingMainUncorSeq])
                df_levelingMain['Linkage Number'] = df_levelingMain['Linkage Number'].astype(str)
                df_levelingMain = df_levelingMain.reset_index(drop=True)
                df_levelingMain['미착공수주잔'] = df_levelingMain.groupby('Linkage Number')['Linkage Number'].transform('size')
                # 특수모듈이면서 메인검사장치를 사용하는 모듈의 조건처리
                df_levelingMain['특수대상'] = ''
                df_spCondition = pd.read_excel(self.list_masterFile[7])
                df_ateP = df_spCondition[df_spCondition['검사호기'] == 'P']
                df_ateP['1차_MAX_그룹'] = df_ateP['1차_MAX_그룹'].fillna(method='ffill')
                df_ateP['2차_MAX_그룹'] = df_ateP['2차_MAX_그룹'].fillna(method='ffill')
                df_ateP['1차_MAX'] = df_ateP['1차_MAX'].fillna(method='ffill')
                df_ateP['2차_MAX'] = df_ateP['2차_MAX'].fillna(method='ffill')
                df_ateP['우선착공'] = df_ateP['우선착공'].fillna(method='ffill')
                df_ateP['특수대상'] = '대상'
                dict_ateP1stCnt = {}
                dict_ateP2ndCnt = {}
                for i in df_ateP.index:
                    if str(df_ateP['1차_MAX_그룹'][i]) != '-' and str(df_ateP['1차_MAX_그룹'][i]) != '' and str(df_ateP['1차_MAX_그룹'][i]) != 'nan':
                        dict_ateP1stCnt[df_ateP['1차_MAX_그룹'][i]] = int(df_ateP['1차_MAX'][i])
                    if str(df_ateP['2차_MAX_그룹'][i]) != '-' and str(df_ateP['2차_MAX_그룹'][i]) != '' and str(df_ateP['2차_MAX_그룹'][i]) != 'nan':
                        dict_ateP2ndCnt[df_ateP['2차_MAX_그룹'][i]] = int(df_ateP['2차_MAX'][i])
                list_ateP = df_ateP['MODEL'].tolist()
                str_where = ""
                for list in list_ateP:
                    str_where += f" OR INSTR(SMT_MS_CODE, '{list}') > 0"
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
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                # if self.isDebug:
                #     df_levelingMain.to_excel('.\\debug\\Main\\flow1.xlsx')
                df_sosFile = pd.read_excel(self.list_masterFile[0])
                df_sosFile['Linkage Number'] = df_sosFile['Linkage Number'].astype(str)
                if self.isDebug:
                    df_sosFile.to_excel('.\\debug\\Main\\flow2.xlsx')
                # 착공 대상 외 모델 삭제
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('ZOTHER')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('YZ')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('SF')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('KM')].index)
                df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('TA80')].index)
                # CT사양은 1차에서만 착공내리도록 처리
                if self.cb_round != '1차':
                    df_sosFile = df_sosFile.drop(df_sosFile[df_sosFile['MS Code'].str.contains('CT')].index)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_sosFile.to_excel('.\\debug\\Main\\flow3.xlsx')
                # 워킹데이 캘린더 불러오기
                dfCalendar = pd.read_excel(self.list_masterFile[4])
                today = datetime.datetime.today().strftime('%Y%m%d')
                if self.isDebug:
                    today = self.date
                # 진척 파일 - SOS2파일 Join
                df_sosFileMerge = pd.merge(df_sosFile, df_levelingMain).drop_duplicates(['Linkage Number'])
                if Path(self.list_masterFile[2]).is_file():
                    df_sosFileMergeSp = pd.merge(df_sosFile, df_levelingSp).drop_duplicates(['Linkage Number'])
                    df_sosFileMerge = pd.concat([df_sosFileMerge, df_sosFileMergeSp])
                else:
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
                # 미착공수주잔이 없는 데이터는 불요이므로 삭제
                df_sosFileMerge = df_sosFileMerge[df_sosFileMerge['미착공수주잔'] != 0]
                # 위 파일을 완성지정일 기준 오름차순 정렬 및 인덱스 재설정
                df_sosFileMerge = df_sosFileMerge.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
                df_sosFileMerge = df_sosFileMerge.reset_index(drop=True)
                # 대표모델 Column 생성
                df_sosFileMerge['대표모델'] = df_sosFileMerge['MS Code'].str[:9]
                # 남은 워킹데이 Column 생성
                df_sosFileMerge['남은 워킹데이'] = 0
                # 긴급오더, 홀딩오더 Linkage Number Column 타입 일치
                df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(str)
                df_holdLinkage['Linkage Number'] = df_holdLinkage['Linkage Number'].astype(str)
                # 긴급오더, 홀딩오더와 위 Sos파일을 Join
                df_MergeLink = pd.merge(df_sosFileMerge, df_emgLinkage, on='Linkage Number', how='left')
                df_Mergemscode = pd.merge(df_sosFileMerge, df_emgmscode, on='MS Code', how='left')
                df_MergeLink = pd.merge(df_MergeLink, df_holdLinkage, on='Linkage Number', how='left')
                df_Mergemscode = pd.merge(df_Mergemscode, df_holdmscode, on='MS Code', how='left')
                df_MergeLink['긴급오더'] = df_MergeLink['긴급오더'].combine_first(df_Mergemscode['긴급오더'])
                df_MergeLink['홀딩오더'] = df_MergeLink['홀딩오더'].combine_first(df_Mergemscode['홀딩오더'])
                df_MergeLink['당일착공'] = ''
                df_MergeLink['완성지정일_원본'] = df_MergeLink['Planned Prod. Completion date']
                # CT사양은 기존 완성지정일보다 4일 더 빠르게 착공내려야 하기 때문에 보정처리
                df_MergeLink.loc[df_MergeLink['MS Code'].str.contains('/CT'), 'Planned Prod. Completion date'] = df_MergeLink['완성지정일_원본'] - datetime.timedelta(days=4)
                df_MergeLink = df_MergeLink.sort_values(by=['Planned Prod. Completion date'], ascending=[True])
                df_MergeLink = df_MergeLink.reset_index(drop=True)
                # 남은 워킹데이 체크 및 컬럼 추가
                for i in df_MergeLink.index:
                    df_MergeLink['남은 워킹데이'][i] = self.checkWorkDay(dfCalendar, today, df_MergeLink['Planned Prod. Completion date'][i])
                    if df_MergeLink['남은 워킹데이'][i] < 1:
                        df_MergeLink['긴급오더'][i] = '대상'
                    elif df_MergeLink['남은 워킹데이'][i] == 1:
                        df_MergeLink['당일착공'][i] = '대상'
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                df_MergeLink['Linkage Number'] = df_MergeLink['Linkage Number'].astype(str)
                # 홀딩오더는 제외
                df_MergeLink = df_MergeLink[df_MergeLink['홀딩오더'].isnull()]
                if self.isDebug:
                    df_MergeLink.to_excel('.\\debug\\Main\\flow4.xlsx')
                # 프로그램 기동날짜의 전일을 계산 (Debug시에는 디버그용 LineEdit에 기록된 날짜를 사용)
                yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                if self.isDebug:
                    yesterday = (datetime.datetime.strptime(self.date, '%Y%m%d') - datetime.timedelta(days=1)).strftime('%Y%m%d')
                # 설정파일 불러오기
                parser = ConfigParser()
                parser.read(self.list_masterFile[16], encoding='euc-kr')
                smtAssyDbHost = parser.get('SMT Assy DB정보', 'Host')
                smtAssyDbPort = parser.getint('SMT Assy DB정보', 'Port')
                smtAssyDbSID = parser.get('SMT Assy DB정보', 'SID')
                smtAssyDbUser = parser.get('SMT Assy DB정보', 'Username')
                smtAssyDbPw = parser.get('SMT Assy DB정보', 'Password')
                # 해당 날짜의 Smt Assy 남은 대수 확인
                df_SmtAssyInven = self.readDB(smtAssyDbHost,
                                                smtAssyDbPort,
                                                smtAssyDbSID,
                                                smtAssyDbUser,
                                                smtAssyDbPw,
                                                "SELECT INV_D, PARTS_NO, CURRENT_INV_QTY FROM pdsg0040 where INV_D = TO_DATE(" + str(yesterday) + ",'YYYYMMDD')")
                df_SmtAssyInven['현재수량'] = 0
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_SmtAssyInven.to_excel('.\\debug\\Main\\flow5.xlsx')
                # 2차 메인피킹 리스트 불러오기 및 Smt Assy 재고량 Df와 Join
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
                    df_joinSmt = pd.merge(df_secOrderList, df_SmtAssyInven, how='right', left_on='ASSY NO', right_on='PARTS_NO')
                    df_joinSmt['대수'] = df_joinSmt['대수'].fillna(0)
                    # Smt Assy 현재 재고량에서 사용량 차감
                    df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY'] - df_joinSmt['대수']
                else:
                    df_joinSmt = df_SmtAssyInven.copy()
                    df_joinSmt['현재수량'] = df_joinSmt['CURRENT_INV_QTY']
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_joinSmt.to_excel('.\\debug\\Main\\flow6.xlsx')
                dict_smtCnt = {}
                # Smt Assy 재고량을 PARTS_NO를 Key로 Dict화
                for i in df_joinSmt.index:
                    if df_joinSmt['현재수량'][i] < 0:
                        df_joinSmt['현재수량'][i] = 0
                    dict_smtCnt[df_joinSmt['PARTS_NO'][i]] = df_joinSmt['현재수량'][i]
                # 설정파일 불러오기
                fam3PdTimeDbHost = parser.get('FAM3공수계산DB 정보', 'Host')
                fam3PdTimeDbPort = parser.getint('FAM3공수계산DB 정보', 'Port')
                fam3PdTimeDbSID = parser.get('FAM3공수계산DB 정보', 'SID')
                fam3PdTimeDbUser = parser.get('FAM3공수계산DB 정보', 'Username')
                fam3PdTimeDbPw = parser.get('FAM3공수계산DB 정보', 'Password')
                # 검사시간DB를 가져옴(공수계산PRG용 DB)
                df_productTime = self.readDB(fam3PdTimeDbHost, fam3PdTimeDbPort, fam3PdTimeDbSID, fam3PdTimeDbUser, fam3PdTimeDbPw, 'SELECT * FROM FAM3_PRODUCT_TIME_TB')
                # 전체 검사시간을 계산
                df_productTime['TotalTime'] = (df_productTime['M_FUNCTION_CHECK'].apply(self.getSec) + df_productTime['A_FUNCTION_CHECK'].apply(self.getSec))
                # 대표모델 컬럼생성 및 중복 제거
                df_productTime['대표모델'] = df_productTime['MODEL'].str[:9]
                df_productTime = df_productTime.drop_duplicates(['대표모델'])
                df_productTime = df_productTime.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_productTime.to_excel('.\\debug\\Main\\flow7.xlsx')
                # 설정파일 불러오기
                pdbsDbHost = parser.get('MSCODE별 SMT Assy DB정보', 'Host')
                pdbsDbPort = parser.getint('MSCODE별 SMT Assy DB정보', 'Port')
                pdbsDbSID = parser.get('MSCODE별 SMT Assy DB정보', 'SID')
                pdbsDbUser = parser.get('MSCODE별 SMT Assy DB정보', 'Username')
                pdbsDbPw = parser.get('MSCODE별 SMT Assy DB정보', 'Password')
                # DB로부터 메인라인의 MSCode별 사용 Smt Assy 가져옴
                df_pdbs = self.readDB(pdbsDbHost,
                                        pdbsDbPort,
                                        pdbsDbSID,
                                        pdbsDbUser,
                                        pdbsDbPw,
                                        "SELECT SMT_MS_CODE, SMT_SMT_ASSY, SMT_CRP_GR_NO FROM sap.pdbs0010 WHERE SMT_CRP_GR_NO = '100L1311'" + str_where)
                # 불필요한 데이터 삭제
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('AST')]
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('BMS')]
                df_pdbs = df_pdbs[~df_pdbs['SMT_MS_CODE'].str.contains('WEB')]
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_pdbs.to_excel('.\\debug\\Main\\flow7-1.xlsx')
                # 사용 Smt Assy를 병렬화
                gb = df_pdbs.groupby('SMT_MS_CODE')
                df_temp = pd.DataFrame([df_pdbs.loc[gb.groups[n], 'SMT_SMT_ASSY'].values for n in gb.groups], index=gb.groups.keys())
                df_temp.columns = ['ROW' + str(i + 1) for i in df_temp.columns]
                rowNo = len(df_temp.columns)
                df_temp = df_temp.reset_index()
                df_temp.rename(columns={'index': 'SMT_MS_CODE'}, inplace=True)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_temp.to_excel('.\\debug\\Main\\flow7-2.xlsx')
                # 검사설비를 List화
                # df_ATEList = df_productTime.copy()
                # df_ATEList = df_ATEList.drop_duplicates(['INSPECTION_EQUIPMENT'])
                # df_ATEList = df_ATEList.reset_index(drop=True)
                # df_ATEList['INSPECTION_EQUIPMENT'] = df_ATEList['INSPECTION_EQUIPMENT'].apply(self.delBackslash)
                # df_ATEList['INSPECTION_EQUIPMENT'] = df_ATEList['INSPECTION_EQUIPMENT'].str.strip()
                df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].apply(self.delBackslash)
                df_productTime['INSPECTION_EQUIPMENT'] = df_productTime['INSPECTION_EQUIPMENT'].str.strip()
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                # if self.isDebug:
                #     df_ATEList.to_excel('.\\debug\\Main\\flow8.xlsx')
                df_ATEList = pd.read_excel(self.list_masterFile[12])
                dict_ate = {}
                list_priorityAte = []
                # 각 검사설비를 Key로 검사시간을 Dict화
                for i in df_ATEList.index:
                    dict_ate[df_ATEList['검사호기 분류'][i]] = (60 * df_ATEList['가동시간'][i]) * 60
                    if str(df_ATEList['설비능력 MAX 사용'][i]) != 'nan' and str(df_ATEList['설비능력 MAX 사용'][i]) != '':
                        list_priorityAte.append(str(df_ATEList['검사호기 분류'][i]))
                # 대표모델 별 검사시간 및 검사설비를 Join
                df_sosAddMainModel = pd.merge(df_MergeLink, df_productTime[['대표모델', 'TotalTime', 'INSPECTION_EQUIPMENT']], on='대표모델', how='left')
                df_sosAddMainModel = df_sosAddMainModel[~df_sosAddMainModel['INSPECTION_EQUIPMENT'].str.contains('None')]
                # 모델별 사용 Smt Assy를 Join
                df_addSmtAssy = pd.merge(df_sosAddMainModel, df_temp, left_on='MS Code', right_on='SMT_MS_CODE', how='left')
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow8-2.xlsx')
                df_addSmtAssy['대표모델별_최소착공필요량_per_일'] = 0
                dict_integCnt = {}
                dict_minContCnt = {}
                # 대표모델 별 최소 착공 필요량을 계산
                for i in df_addSmtAssy.index:
                    # 특수모듈의 경우, 대표모델을 특수모듈 조건표의 그룹명으로 설정
                    if str(df_addSmtAssy['1차_MAX_그룹'][i]) != '' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != '-' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != 'nan':
                        df_addSmtAssy['대표모델'][i] = df_addSmtAssy['1차_MAX_그룹'][i]
                    # 각 대표모델 별 적산착공량을 계산하여 딕셔너리에 저장
                    if df_addSmtAssy['대표모델'][i] in dict_integCnt:
                        dict_integCnt[df_addSmtAssy['대표모델'][i]] += int(df_addSmtAssy['미착공수주잔'][i])
                    else:
                        dict_integCnt[df_addSmtAssy['대표모델'][i]] = int(df_addSmtAssy['미착공수주잔'][i])
                    # 이미 완성지정일을 지난경우, 워킹데이 계산을 위해 워킹데이를 1로 설정
                    if df_addSmtAssy['남은 워킹데이'][i] <= 0:
                        workDay = 1
                    else:
                        workDay = df_addSmtAssy['남은 워킹데이'][i]
                    # 완성지정일별 최소필요착공량 계산 후, 딕셔너리에 리스트(대표모델, 완성지정일)로 저장
                    if str(df_addSmtAssy['1차_MAX_그룹'][i]) != '' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != 'nan' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != '-':
                        dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [df_addSmtAssy['1차_MAX'][i], df_addSmtAssy['Planned Prod. Completion date'][i]]
                    elif len(dict_minContCnt) > 0:
                        if df_addSmtAssy['대표모델'][i] in dict_minContCnt:
                            if dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] < math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay):
                                dict_minContCnt[df_addSmtAssy['대표모델'][i]][0] = math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay)
                                dict_minContCnt[df_addSmtAssy['대표모델'][i]][1] = df_addSmtAssy['Planned Prod. Completion date'][i]
                        else:
                            dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay), df_addSmtAssy['Planned Prod. Completion date'][i]]
                    else:
                        dict_minContCnt[df_addSmtAssy['대표모델'][i]] = [math.ceil(dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay), df_addSmtAssy['Planned Prod. Completion date'][i]]
                    if workDay <= 0:
                        workDay = 1
                    # 위에서 계산한 최소필요착공량을 컬럼화시켜 데이터프레임에 입력
                    if str(df_addSmtAssy['1차_MAX_그룹'][i]) != '' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != 'nan' and str(df_addSmtAssy['1차_MAX_그룹'][i]) != '-':
                        df_addSmtAssy['대표모델별_최소착공필요량_per_일'][i] = df_addSmtAssy['1차_MAX'][i]
                    else:
                        df_addSmtAssy['대표모델별_최소착공필요량_per_일'][i] = dict_integCnt[df_addSmtAssy['대표모델'][i]] / workDay
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                # 검사설비능력 최적화 후, 분할된 Row를 다시 합치기 위해 분할 전 데이터를 복사하여 저장
                df_flow9 = df_addSmtAssy.copy()
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow9.xlsx')
                df_addSmtAssyCopy = pd.DataFrame(columns=df_addSmtAssy.columns)
                # 검사설비능력 최적화를 위하여 미착공수주잔을 20대씩 분할 시킴. (ex. 83대 일 경우, Row를 20, 20, 20, 20, 3으로 5분할)
                for i in df_addSmtAssy.index:
                    div = df_addSmtAssy['미착공수주잔'][i] // 20
                    mod = df_addSmtAssy['미착공수주잔'][i] % 20
                    df_temp = pd.DataFrame(columns=df_addSmtAssy.columns)
                    for j in range(0, div + 1):
                        if j != div:
                            df_temp = df_temp.append(df_addSmtAssy.iloc[i])
                            df_temp = df_temp.reset_index(drop=True)
                            df_temp['미착공수주잔'][j] = 20
                        elif mod > 0:
                            df_temp = df_temp.append(df_addSmtAssy.iloc[i])
                            df_temp = df_temp.reset_index(drop=True)
                            df_temp['미착공수주잔'][j] = mod
                    df_addSmtAssyCopy = pd.concat([df_addSmtAssyCopy, df_temp])
                df_addSmtAssy = df_addSmtAssyCopy.reset_index(drop=True)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow9-1.xlsx')
                dict_minContCopy = dict_minContCnt.copy()
                # 대표모델 별 최소착공 필요량을 기준으로 평준화 적용 착공량을 계산. 미착공수주잔에서 해당 평준화 적용 착공량을 제외한 수량은 잔여착공량으로 기재
                df_addSmtAssy['평준화_적용_착공량'] = 0
                for i in df_addSmtAssy.index:
                    if df_addSmtAssy['긴급오더'][i] == '대상':
                        df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
                        if df_addSmtAssy['대표모델'][i] in dict_minContCopy:
                            if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
                                dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
                            else:
                                dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
                    elif df_addSmtAssy['대표모델'][i] in dict_minContCopy:
                        if dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] >= int(df_addSmtAssy['미착공수주잔'][i]):
                            df_addSmtAssy['평준화_적용_착공량'][i] = int(df_addSmtAssy['미착공수주잔'][i])
                            dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] -= int(df_addSmtAssy['미착공수주잔'][i])
                        else:
                            df_addSmtAssy['평준화_적용_착공량'][i] = dict_minContCopy[df_addSmtAssy['대표모델'][i]][0]
                            dict_minContCopy[df_addSmtAssy['대표모델'][i]][0] = 0
                df_addSmtAssy['잔여_착공량'] = df_addSmtAssy['미착공수주잔'] - df_addSmtAssy['평준화_적용_착공량']
                df_addSmtAssy = df_addSmtAssy.sort_values(by=['우선착공', '긴급오더', '당일착공', 'Planned Prod. Completion date', '평준화_적용_착공량'], ascending=[False, False, False, True, False])
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow10.xlsx')
                df_addSmtAssy['SMT반영_착공량'] = 0
                # 알람 상세 DataFrame 생성
                df_alarmDetail = pd.DataFrame(columns=["No.", "분류", "L/N", "MS CODE", "SMT ASSY", "수주수량", "부족수량", "검사호기", "대상 검사시간(초)", "필요시간(초)", "완성예정일"])
                alarmDetailNo = 1
                # 최소착공량에 대해 Smt적용 착공량 계산
                df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, False, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo)
                if self.isDebug:
                    df_alarmDetail.to_excel('.\\debug\\Main\\df_alarmDetail.xlsx')
                # 잔여 착공량에 대해 Smt적용 착공량 계산
                df_addSmtAssy['SMT반영_착공량_잔여'] = 0
                df_addSmtAssy, dict_smtCnt, alarmDetailNo, df_alarmDetail = self.smtReflectInst(df_addSmtAssy, True, dict_smtCnt, alarmDetailNo, df_alarmDetail, rowNo)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow11.xlsx')
                df_addSmtAssy['임시수량'] = 0
                df_addSmtAssy['설비능력반영_착공량'] = 0
                df_addSmtAssy['임시수량_잔여'] = 0
                df_addSmtAssy['설비능력반영_착공량_잔여'] = 0
                df_addSmtAssy['남은착공량'] = 0
                df_addSmtAssy = df_addSmtAssy.reset_index()
                # 검사설비 우선사용 계산을 위해 데이터프레임 신규 선언
                df_priority = pd.DataFrame(columns=df_addSmtAssy.columns)
                df_unPriority = pd.DataFrame(columns=df_addSmtAssy.columns)
                # 우선사용 검사설비 대상을 찾아 우선사용/비사용 데이터 프레임으로 분할
                if len(list_priorityAte) > 0:
                    for ate in list_priorityAte:
                        df_priority = pd.concat([df_priority, df_addSmtAssy[df_addSmtAssy['INSPECTION_EQUIPMENT'].str.contains(ate)]])
                        df_priority = pd.concat([df_priority, df_addSmtAssy[df_addSmtAssy['긴급오더'].notnull()]])
                        df_priority = pd.concat([df_priority, df_addSmtAssy[df_addSmtAssy['당일착공'].str.contains('대상')]])
                        if len(df_unPriority) > 0:
                            df_unPriority = pd.merge(df_unPriority, df_addSmtAssy[~df_addSmtAssy['INSPECTION_EQUIPMENT'].str.contains(ate)], how='inner')
                            df_unPriority = df_unPriority[df_unPriority['긴급오더'].isnull()]
                            df_unPriority = df_unPriority[~df_unPriority['당일착공'].str.contains('대상')]
                            df_unPriority = df_unPriority.drop_duplicates(['index'])
                        else:
                            df_unPriority = df_addSmtAssy[~df_addSmtAssy['INSPECTION_EQUIPMENT'].str.contains(ate)]
                            df_unPriority = df_unPriority[df_unPriority['긴급오더'].isnull()]
                            df_unPriority = df_unPriority[~df_unPriority['당일착공'].str.contains('대상')]
                            df_unPriority = df_unPriority.drop_duplicates(['index'])
                        df_priority = df_priority.drop_duplicates(['index'])
                else:
                    df_unPriority = df_addSmtAssy.copy()
                if self.isDebug:
                    df_priority.to_excel('.\\debug\\Main\\flow11-1.xlsx')
                    df_unPriority.to_excel('.\\debug\\Main\\flow11-2.xlsx')
                # CT제한 조건표를 불러오기
                df_limitCtCond = pd.read_excel(self.list_masterFile[13])
                limitCtCnt = df_limitCtCond[df_limitCtCond['상세구분'] == 'MAIN']['허용수량'].values[0]
                # 설비능력 반영 착공량 계산
                # print(df_priority.head())
                # print(df_unPriority.head())
                df_priority, dict_ate, alarmDetailNo, df_alarmDetail, self.moduleMaxCnt, limitCtCnt = self.ateReflectInst(df_priority, False, dict_ate, df_alarmDetail, alarmDetailNo, self.moduleMaxCnt, limitCtCnt)
                # if self.isDebug:
                #     df_priority.to_excel('.\\debug\\Main\\flow11-3.xlsx')
                # 잔여 착공량에 대해 설비능력 반영 착공량 계산
                df_priority, dict_ate, alarmDetailNo, df_alarmDetail, self.moduleMaxCnt, limitCtCnt = self.ateReflectInst(df_priority, True, dict_ate, df_alarmDetail, alarmDetailNo, self.moduleMaxCnt, limitCtCnt)
                # 설비능력 반영 착공량 계산
                df_unPriority, dict_ate, alarmDetailNo, df_alarmDetail, self.moduleMaxCnt, limitCtCnt = self.ateReflectInst(df_unPriority, False, dict_ate, df_alarmDetail, alarmDetailNo, self.moduleMaxCnt, limitCtCnt)
                # 잔여 착공량에 대해 설비능력 반영 착공량 계산
                df_unPriority, dict_ate, alarmDetailNo, df_alarmDetail, self.moduleMaxCnt, limitCtCnt = self.ateReflectInst(df_unPriority, True, dict_ate, df_alarmDetail, alarmDetailNo, self.moduleMaxCnt, limitCtCnt)
                # 검사설비 우선사용/비사용 데이터프레임을 다시 통합.
                df_addSmtAssy = pd.concat([df_priority, df_unPriority])
                # 잔여 검사설비능력 출력을 위하여 데이터프레임 선언
                df_dict = pd.DataFrame(data=dict_ate, index=[0])
                df_dict = df_dict.T
                if self.isDebug:
                    df_dict.to_excel('.\\debug\\Main\\dict_ate.xlsx')
                df_dict = df_dict.reset_index()
                df_dict.columns = ['검사설비', '남은 시간']
                df_dict['남은 시간'] = df_dict['남은 시간'].apply(self.convertSecToTime)
                df_dict = pd.merge(df_dict, df_ATEList, how='left', left_on='검사설비', right_on='검사호기 분류')
                df_dict = df_dict[['검사설비', '가동시간', '남은 시간']]
                now = time.strftime('%H%M%S')
                # 현재 시간의 잔여검사설비 출력
                if not os.path.exists(f'.\\Output\\Result\\{str(today)}'):
                    os.makedirs(f'.\\Output\\Result\\{str(today)}')
                if not os.path.exists(f'.\\Output\\Result\\{str(today)}\\{self.cb_round}'):
                    os.makedirs(f'.\\Output\\Result\\{str(today)}\\{self.cb_round}')
                df_dict.to_excel(f'.\\Output\\Result\\\\{str(today)}\\{str(self.cb_round)}\\잔여검사설비능력_{now}.xlsx')
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow12.xlsx')
                    df_alarmDetail = df_alarmDetail.reset_index(drop=True)
                    df_alarmDetail.to_excel('.\\debug\\Main\\df_alarmDetail.xlsx')
                # 알람 상세 결과에서 각 항목별로 요약
                if len(df_alarmDetail) > 0:
                    # 분류1 요약
                    df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
                    df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum()
                    df_firstAlarmSummary = df_firstAlarmSummary.reset_index()
                    df_firstAlarmSummary['수량'] = df_firstAlarmSummary['부족수량']
                    df_firstAlarmSummary['분류'] = '1'
                    df_firstAlarmSummary['MS CODE'] = '-'
                    df_firstAlarmSummary['검사호기'] = '-'
                    df_firstAlarmSummary['부족 시간'] = '-'
                    df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
                    del df_firstAlarmSummary['부족수량']
                    # 분류2 요약
                    df_secAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2']
                    df_secAlarmSummary = df_secAlarm.groupby("검사호기")['필요시간(초)'].sum()
                    df_secAlarmSummary = df_secAlarmSummary.reset_index()
                    df_secAlarmSummary['부족 시간'] = df_secAlarmSummary['필요시간(초)']
                    df_secAlarmSummary['부족 시간'] = df_secAlarmSummary['부족 시간'].apply(self.convertSecToTime)
                    df_secAlarmSummary['분류'] = '2'
                    df_secAlarmSummary['MS CODE'] = '-'
                    df_secAlarmSummary['SMT ASSY'] = '-'
                    df_secAlarmSummary['수량'] = '-'
                    df_secAlarmSummary['Message'] = '검사설비능력이 부족합니다. 생산 가능여부를 확인해 주세요.'
                    del df_secAlarmSummary['필요시간(초)']
                    # 분류 기타2 요약
                    df_etc2Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타2']
                    df_etc2AlarmSummary = df_etc2Alarm.groupby('MS CODE')['부족수량'].sum()
                    df_etc2AlarmSummary = df_etc2AlarmSummary.reset_index()
                    df_etc2AlarmSummary['수량'] = df_etc2AlarmSummary['부족수량']
                    df_etc2AlarmSummary['분류'] = '기타2'
                    df_etc2AlarmSummary['SMT ASSY'] = '-'
                    df_etc2AlarmSummary['검사호기'] = '-'
                    df_etc2AlarmSummary['부족 시간'] = '-'
                    df_etc2AlarmSummary['Message'] = '긴급오더 및 당일착공 대상의 총 착공량이 입력한 최대착공량보다 큽니다. 최대착공량을 확인해주세요.'
                    del df_etc2AlarmSummary['부족수량']
                    # 분류 기타4 요약
                    df_etc4Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타4']
                    df_etc4AlarmSummary = df_etc4Alarm.groupby('MS CODE')['부족수량'].sum()
                    df_etc4AlarmSummary = df_etc4AlarmSummary.reset_index()
                    df_etc4AlarmSummary['수량'] = df_etc4AlarmSummary['부족수량']
                    df_etc4AlarmSummary['분류'] = '기타4'
                    df_etc4AlarmSummary['SMT ASSY'] = '-'
                    df_etc4AlarmSummary['검사호기'] = '-'
                    df_etc4AlarmSummary['부족 시간'] = '-'
                    df_etc4AlarmSummary['Message'] = '설정된 CT 제한대수보다 최소 착공 필요량이 많습니다. 설정된 CT 제한대수를 확인해주세요.'
                    # 위 알람을 병합
                    df_alarmSummary = pd.concat([df_firstAlarmSummary, df_secAlarmSummary, df_etc2AlarmSummary, df_etc4AlarmSummary])
                    # 기타 알람에 대한 추가
                    df_etcList = df_alarmDetail[(df_alarmDetail['분류'] == '기타1') | (df_alarmDetail['분류'] == '기타3')]
                    df_etcList = df_etcList.drop_duplicates(['MS CODE'])
                    for i in df_etcList.index:
                        if df_etcList['분류'][i] == '기타1':
                            df_alarmSummary = pd.concat([df_alarmSummary,
                                                        pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                                    "MS CODE": df_etcList['MS CODE'][i],
                                                                                    "SMT ASSY": '-',
                                                                                    "수량": 0,
                                                                                    "검사호기": '-',
                                                                                    "부족 시간": 0,
                                                                                    "Message": '해당 MS CODE에서 사용되는 SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'}])])
                        elif df_etcList['분류'][i] == '기타3':
                            df_alarmSummary = pd.concat([df_alarmSummary,
                                                        pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                                    "MS CODE": df_etcList['MS CODE'][i],
                                                                                    "SMT ASSY": '-',
                                                                                    "수량": 0,
                                                                                    "검사호기": '-',
                                                                                    "부족 시간": 0,
                                                                                    "Message": 'SMT ASSY 정보가 등록되지 않아 재고를 확인할 수 없습니다. 등록 후 다시 실행해주세요.'}])])
                    df_alarmSummary = df_alarmSummary.reset_index(drop=True)
                    df_alarmSummary = df_alarmSummary[['분류', 'MS CODE', 'SMT ASSY', '수량', '검사호기', '부족 시간', 'Message']]
                    if self.isDebug:
                        df_alarmSummary.to_excel('.\\debug\\Main\\df_alarmSummary.xlsx')
                    if not os.path.exists(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}'):
                        os.makedirs(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}')
                    df_alarmExplain = pd.DataFrame({'분류': ['1', '2', '기타1', '기타2', '기타3', '기타4'],
                                                            '분류별 상황': ['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우',
                                                            '당일 착공분(or 긴급착공분)에 대해 검사설비 능력이 부족할 경우',
                                                            'MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우',
                                                            '긴급오더 대상 착공시 최대착공량(사용자입력공수)이 부족할 경우',
                                                            'SMT ASSY 정보가 DB에 미등록된 경우',
                                                            '당일 최소 착공필요량 > CT제한 대수인 경우']})
                    # 파일 한개로 출력
                    with pd.ExcelWriter(f'.\\Output\\Alarm\\{str(today)}\\{self.cb_round}\\FAM3_AlarmList_{str(today)}_Main.xlsx') as writer:
                        df_alarmSummary.to_excel(writer, sheet_name='정리', index=True)
                        df_alarmDetail.to_excel(writer, sheet_name='상세', index=True)
                        df_alarmExplain.to_excel(writer, sheet_name='설명', index=False)
                addColumnList = ['평준화_적용_착공량', '잔여_착공량', 'SMT반영_착공량', 'SMT반영_착공량_잔여', '설비능력반영_착공량', '설비능력반영_착공량_잔여']
                # 20개씩 분할되었던 Row를 하나로 통합하는 작업 실시
                for column in addColumnList:
                    df_group = df_addSmtAssy.groupby('Linkage Number')[column].sum()
                    df_group = df_group.reset_index()
                    df_group.columns = ['Linkage Number', column]
                    df_flow9 = pd.merge(df_flow9, df_group[['Linkage Number', column]], on='Linkage Number', how='left')
                    df_flow9[column] = df_flow9[column].fillna(0)
                df_addSmtAssy = df_flow9.copy()
                df_addSmtAssy = df_addSmtAssy.sort_values(by=['우선착공', '긴급오더', '당일착공', 'Planned Prod. Completion date', '설비능력반영_착공량', '설비능력반영_착공량_잔여'], ascending=[False, False, False, True, False, False])
                df_addSmtAssy = df_addSmtAssy.reset_index(drop=True)
                df_addSmtAssy['MODEL'] = df_addSmtAssy['MS Code'].str[:6]
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow12-1.xlsx')
                # 총착공량 컬럼으로 병합
                df_addSmtAssy['총착공량'] = df_addSmtAssy['설비능력반영_착공량'] + df_addSmtAssy['설비능력반영_착공량_잔여']
                df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['총착공량'] != 0]
                # 홀딩리스트 파일 불러오기
                df_holdingList = pd.read_excel(self.list_masterFile[17])
                # 홀딩리스트와 비교하여 조건에 해당하는 경우, 알람 메시지 출력
                for i in df_holdingList.index:
                    message = ""
                    if len(df_addSmtAssy[df_addSmtAssy['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values) > 0:
                        totalCnt = 0
                        for cnt in df_addSmtAssy[df_addSmtAssy['MODEL'] == df_holdingList['MODEL'][i]]['총착공량'].values:
                            totalCnt += cnt
                        message = f"{df_holdingList['MODEL'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                    if len(df_addSmtAssy[df_addSmtAssy['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values) > 0:
                        message = f"{df_holdingList['LINKAGENO'][i]} {int(df_addSmtAssy[df_addSmtAssy['Linkage Number'] == df_holdingList['LINKAGENO'][i]]['총착공량'].values[0])}대 {df_holdingList['REMARK'][i]}"
                    if len(df_addSmtAssy[df_addSmtAssy['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values) > 0:
                        totalCnt = 0
                        for cnt in df_addSmtAssy[df_addSmtAssy['MS Code'] == df_holdingList['MS-CODE'][i]]['총착공량'].values:
                            totalCnt += cnt
                        message = f"{df_holdingList['MS-CODE'][i]} {int(totalCnt)}대 {df_holdingList['REMARK'][i]}"
                    if len(message) > 0:
                        self.returnWarning.emit(message)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_addSmtAssy.to_excel('.\\debug\\Main\\flow13.xlsx')
                df_returnSp = df_addSmtAssy[df_addSmtAssy['특수대상'] == '대상']
                self.mainReturnDf.emit(df_returnSp)
                df_addSmtAssy = df_addSmtAssy[df_addSmtAssy['특수대상'] != '대상']
                # 최대착공량만큼 착공 못했을 경우, 메시지 출력
                if self.moduleMaxCnt > 0:
                    self.returnWarning.emit(f'아직 착공하지 못한 모델이 [{int(self.moduleMaxCnt)}대] 남았습니다. 설비능력 부족이 예상됩니다. 확인해주세요.')
                # 레벨링 리스트와 병합
                df_addSmtAssy = df_addSmtAssy.astype({'Linkage Number': 'str'})
                df_levelingMain = df_levelingMain.astype({'Linkage Number': 'str'})
                df_mergeOrder = pd.merge(df_addSmtAssy, df_levelingMain, on='Linkage Number', how='left')
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrder.to_excel('.\\debug\\Main\\flow14.xlsx')
                df_mergeOrderResult = pd.DataFrame().reindex_like(df_mergeOrder)
                df_mergeOrderResult = df_mergeOrderResult[0:0]
                # 총착공량 만큼 개별화
                for i in df_addSmtAssy.index:
                    for j in df_mergeOrder.index:
                        if df_addSmtAssy['Linkage Number'][i] == df_mergeOrder['Linkage Number'][j]:
                            if j > 0:
                                if df_mergeOrder['Linkage Number'][j] != df_mergeOrder['Linkage Number'][j - 1]:
                                    orderCnt = int(df_addSmtAssy['총착공량'][i])
                            else:
                                orderCnt = int(df_addSmtAssy['총착공량'][i])
                            if orderCnt > 0:
                                df_mergeOrderResult = df_mergeOrderResult.append(df_mergeOrder.iloc[j])
                                orderCnt -= 1
                # 사이클링을 위해 검사설비별로 정리
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['INSPECTION_EQUIPMENT'], ascending=[False])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Main\\flow15.xlsx')
                # 긴급오더 제외하고 사이클 대상만 식별하여 검사장치별로 갯수 체크
                df_unCt = df_mergeOrderResult[df_mergeOrderResult['MS Code'].str.contains('/CT')]
                df_mergeOrderResult = df_mergeOrderResult[~df_mergeOrderResult['MS Code'].str.contains('/CT')]
                df_cycleCopy = df_mergeOrderResult[df_mergeOrderResult['긴급오더'].isnull()]
                df_cycleCopy['검사장치Cnt'] = df_cycleCopy.groupby('INSPECTION_EQUIPMENT')['INSPECTION_EQUIPMENT'].transform('size')
                df_cycleCopy = df_cycleCopy.sort_values(by=['검사장치Cnt'], ascending=[False])
                df_cycleCopy = df_cycleCopy.reset_index(drop=True)
                # 긴급오더 포함한 Df와 병합
                df_mergeOrderResult = pd.merge(df_mergeOrderResult, df_cycleCopy[['Planned Order', '검사장치Cnt']], on='Planned Order', how='left')
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['검사장치Cnt'], ascending=[False])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Main\\flow15-1.xlsx')
                # 최대 사이클 번호 체크
                maxCycle = float(df_cycleCopy['검사장치Cnt'][0])
                cycleGr = 1.0
                df_mergeOrderResult['사이클그룹'] = 0
                # 각 검사장치별로 사이클 그룹을 작성하고, 최대 사이클과 비교하여 각 사이클그룹에서 배수처리
                for i in df_mergeOrderResult.index:
                    if df_mergeOrderResult['긴급오더'][i] != '대상':
                        multiCnt = maxCycle / df_mergeOrderResult['검사장치Cnt'][i]
                        if i == 0:
                            df_mergeOrderResult['사이클그룹'][i] = cycleGr
                        else:
                            if df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] != df_mergeOrderResult['INSPECTION_EQUIPMENT'][i - 1]:
                                if i == 1:
                                    cycleGr = 2.0
                                else:
                                    cycleGr = 1.0
                            df_mergeOrderResult['사이클그룹'][i] = cycleGr * multiCnt
                        cycleGr += 1.0
                    if cycleGr >= maxCycle:
                        cycleGr = 1.0
                # 배정된 사이클 그룹 순으로 정렬
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['사이클그룹'], ascending=[True])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Main\\flow16.xlsx')
                df_mergeOrderResult = df_mergeOrderResult.reset_index()
                # 연속으로 같은 검사설비가 오지 않도록 순서를 재조정
                for i in df_mergeOrderResult.index:
                    if df_mergeOrderResult['긴급오더'][i] != '대상':
                        if (i != 0 and (df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] == df_mergeOrderResult['INSPECTION_EQUIPMENT'][i - 1])):
                            for j in df_mergeOrderResult.index:
                                if df_mergeOrderResult['긴급오더'][j] != '대상':
                                    if ((j != 0 and j < len(df_mergeOrderResult) - 1) and (df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] != df_mergeOrderResult['INSPECTION_EQUIPMENT'][j + 1]) and (df_mergeOrderResult['INSPECTION_EQUIPMENT'][i] != df_mergeOrderResult['INSPECTION_EQUIPMENT'][j])):
                                        df_mergeOrderResult['index'][i] = (float(df_mergeOrderResult['index'][j]) + float(df_mergeOrderResult['index'][j + 1])) / 2
                                        df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['index'], ascending=[True])
                                        df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                                        break
                df_unCt['index'] = 0
                df_unCt['사이클그룹'] = 0
                # CT대상인 모델과 비대상 모델을 다시 병합. (CT는 사이클그룹이 가장최상위)
                df_mergeOrderResult = pd.concat([df_unCt, df_mergeOrderResult])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                progress += round(maxPb / 21)
                self.returnPb.emit(progress)
                if self.isDebug:
                    df_mergeOrderResult.to_excel('.\\debug\\Main\\flow17.xlsx')
                df_mergeOrderResult['No (*)'] = int(maxNo) + (df_mergeOrderResult.index.astype(int) + 1) * 10
                df_mergeOrderResult['Scheduled Start Date (*)'] = self.constDate
                df_mergeOrderResult['Planned Order'] = df_mergeOrderResult['Planned Order'].astype(int).astype(str).str.zfill(10)
                df_mergeOrderResult['Scheduled End Date'] = df_mergeOrderResult['Scheduled End Date'].astype(str).str.zfill(10)
                df_mergeOrderResult['Specified Start Date'] = df_mergeOrderResult['Specified Start Date'].astype(str).str.zfill(10)
                df_mergeOrderResult['Specified End Date'] = df_mergeOrderResult['Specified End Date'].astype(str).str.zfill(10)
                df_mergeOrderResult['Spec Freeze Date'] = df_mergeOrderResult['Spec Freeze Date'].astype(str).str.zfill(10)
                df_mergeOrderResult['Component Number'] = df_mergeOrderResult['Component Number'].astype(int).astype(str).str.zfill(4)
                df_mergeOrderResult = df_mergeOrderResult[['No (*)',
                                                            'Sequence No',
                                                            'Production Order',
                                                            'Planned Order',
                                                            'Manual',
                                                            'Scheduled Start Date (*)',
                                                            'Scheduled End Date',
                                                            'Specified Start Date',
                                                            'Specified End Date',
                                                            'Demand destination country',
                                                            'MS-CODE',
                                                            'Allocate',
                                                            'Spec Freeze Date',
                                                            'Linkage Number',
                                                            'Order Number',
                                                            'Order Item',
                                                            'Combination flag',
                                                            'Project Definition',
                                                            'Error message',
                                                            'Leveling Group',
                                                            'Leveling Class',
                                                            'Planning Plant',
                                                            'Component Number',
                                                            'Serial Number']]
                dict_emgLinkage = {}
                dict_emgMscode = {}
                for i in df_emgLinkage.index:
                    if len(df_mergeOrderResult[df_mergeOrderResult['Linkage Number'] == df_emgLinkage['Linkage Number'][i]]['Linkage Number'].values) > 0:
                        dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = True
                    else:
                        dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = False
                for i in df_emgmscode.index:
                    if len(df_mergeOrderResult[df_mergeOrderResult['MS Code'] == df_emgmscode['MS-CODE'][i]]['MS Code'].values) > 0:
                        dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = True
                    else:
                        dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = False

                self.mainReturnEmgLinkage.emit(dict_emgLinkage)
                self.mainReturnEmgMscode.emit(dict_emgMscode)

                progress += round(maxPb / 21)
                self.returnPb.emit(progress)

                outputFile = f'.\\Output\\Result\\{str(today)}\\{self.cb_round}\\{str(today)}_Main.xlsx'
                df_mergeOrderResult.to_excel(outputFile, index=False)
            else:
                df_returnSp = pd.DataFrame()
                self.mainReturnDf.emit(df_returnSp)
                self.returnPb.emit(maxPb)
            # if self.isDebug:
            end = time.time()
            print(f"{end - start:.5f} sec")
            self.returnEnd.emit(True)
            return
        except Exception as e:
            self.returnError.emit(e)
            return
