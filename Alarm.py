import pandas as pd
import datetime
import os


class Alarm():
    def __init__(self, productClass=None):
        super().__init__()
        self.isDebug = productClass.isDebug
        self.className = type(productClass).__name__    # Class name of the product
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SPThread':
            self.cb_round = productClass.cb_round
        elif self.className == 'JuxtaOldThread':
            self.cb_round = ''
        self.date = productClass.date

    # 초단위의 시간을 시/분/초로 분할
    def convertSecToTime(self, seconds):
        seconds = seconds % (24 * 3600)
        hour = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60
        return "%d:%02d:%02d" % (hour, minutes, seconds)

    # def summary(self, df_alarmDetail):
    #     today = datetime.datetime.today().strftime('%Y%m%d')
    #     if len(df_alarmDetail) > 0:
    #         if self.className == 'Fam3MainThread':
    #             # 분류1 요약
    #             df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
    #             df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum()
    #             df_firstAlarmSummary = df_firstAlarmSummary.reset_index()
    #             df_firstAlarmSummary['수량'] = df_firstAlarmSummary['부족수량']
    #             df_firstAlarmSummary['분류'] = '1'
    #             df_firstAlarmSummary['MS CODE'] = '-'
    #             df_firstAlarmSummary['검사호기'] = '-'
    #             df_firstAlarmSummary['부족 시간'] = '-'
    #             df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
    #             del df_firstAlarmSummary['부족수량']
    #             # 분류2 요약
    #             df_secAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2']
    #             df_secAlarmSummary = df_secAlarm.groupby("검사호기")['필요시간(초)'].sum()
    #             df_secAlarmSummary = df_secAlarmSummary.reset_index()
    #             df_secAlarmSummary['부족 시간'] = df_secAlarmSummary['필요시간(초)']
    #             df_secAlarmSummary['부족 시간'] = df_secAlarmSummary['부족 시간'].apply(self.convertSecToTime)
    #             df_secAlarmSummary['분류'] = '2'
    #             df_secAlarmSummary['MS CODE'] = '-'
    #             df_secAlarmSummary['SMT ASSY'] = '-'
    #             df_secAlarmSummary['수량'] = '-'
    #             df_secAlarmSummary['Message'] = '검사설비능력이 부족합니다. 생산 가능여부를 확인해 주세요.'
    #             del df_secAlarmSummary['필요시간(초)']
    #             # 분류 기타2 요약
    #             df_etc2Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타2']
    #             df_etc2AlarmSummary = df_etc2Alarm.groupby('MS CODE')['부족수량'].sum()
    #             df_etc2AlarmSummary = df_etc2AlarmSummary.reset_index()
    #             df_etc2AlarmSummary['수량'] = df_etc2AlarmSummary['부족수량']
    #             df_etc2AlarmSummary['분류'] = '기타2'
    #             df_etc2AlarmSummary['SMT ASSY'] = '-'
    #             df_etc2AlarmSummary['검사호기'] = '-'
    #             df_etc2AlarmSummary['부족 시간'] = '-'
    #             df_etc2AlarmSummary['Message'] = '긴급오더 및 당일착공 대상의 총 착공량이 입력한 최대착공량보다 큽니다. 최대착공량을 확인해주세요.'
    #             del df_etc2AlarmSummary['부족수량']
    #             # 분류 기타4 요약
    #             df_etc4Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타4']
    #             df_etc4AlarmSummary = df_etc4Alarm.groupby('MS CODE')['부족수량'].sum()
    #             df_etc4AlarmSummary = df_etc4AlarmSummary.reset_index()
    #             df_etc4AlarmSummary['수량'] = df_etc4AlarmSummary['부족수량']
    #             df_etc4AlarmSummary['분류'] = '기타4'
    #             df_etc4AlarmSummary['SMT ASSY'] = '-'
    #             df_etc4AlarmSummary['검사호기'] = '-'
    #             df_etc4AlarmSummary['부족 시간'] = '-'
    #             df_etc4AlarmSummary['Message'] = '설정된 CT 제한대수보다 최소 착공 필요량이 많습니다. 설정된 CT 제한대수를 확인해주세요.'
    #             # 위 알람을 병합
    #             df_alarmSummary = pd.concat([df_firstAlarmSummary, df_secAlarmSummary, df_etc2AlarmSummary, df_etc4AlarmSummary])
    #             # 기타 알람에 대한 추가
    #             df_etcList = df_alarmDetail[(df_alarmDetail['분류'] == '기타1') | (df_alarmDetail['분류'] == '기타3')]
    #             df_etcList = df_etcList.drop_duplicates(['MS CODE'])
    #             for i in df_etcList.index:
    #                 if df_etcList['분류'][i] == '기타1':
    #                     df_alarmSummary = pd.concat([df_alarmSummary,
    #                                                 pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
    #                                                                             "MS CODE": df_etcList['MS CODE'][i],
    #                                                                             "SMT ASSY": '-',
    #                                                                             "수량": 0,
    #                                                                             "검사호기": '-',
    #                                                                             "부족 시간": 0,
    #                                                                             "Message": '해당 MS CODE에서 사용되는 SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'}])])
    #                 elif df_etcList['분류'][i] == '기타3':
    #                     df_alarmSummary = pd.concat([df_alarmSummary,
    #                                                 pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
    #                                                                             "MS CODE": df_etcList['MS CODE'][i],
    #                                                                             "SMT ASSY": '-',
    #                                                                             "수량": 0,
    #                                                                             "검사호기": '-',
    #                                                                             "부족 시간": 0,
    #                                                                             "Message": 'SMT ASSY 정보가 등록되지 않아 재고를 확인할 수 없습니다. 등록 후 다시 실행해주세요.'}])])
    #             df_alarmSummary = df_alarmSummary.reset_index(drop=True)
    #             df_alarmSummary = df_alarmSummary[['분류', 'MS CODE', 'SMT ASSY', '수량', '검사호기', '부족 시간', 'Message']]
    #             if self.isDebug:
    #                 df_alarmSummary.to_excel('.\\debug\\FAM3\\Main\\df_alarmSummary.xlsx')
    #             if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}'):
    #                 os.makedirs(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}')
    #             df_alarmExplain = pd.DataFrame({'분류': ['1', '2', '기타1', '기타2', '기타3', '기타4'],
    #                                                     '분류별 상황': ['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우',
    #                                                     '당일 착공분(or 긴급착공분)에 대해 검사설비 능력이 부족할 경우',
    #                                                     'MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우',
    #                                                     '긴급오더 대상 착공시 최대착공량(사용자입력공수)이 부족할 경우',
    #                                                     'SMT ASSY 정보가 DB에 미등록된 경우',
    #                                                     '당일 최소 착공필요량 > CT제한 대수인 경우']})
    #             # 파일 한개로 출력
    #             with pd.ExcelWriter(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}\\FAM3_AlarmList_{str(today)}_Main.xlsx') as writer:
    #                 df_alarmSummary.to_excel(writer, sheet_name='정리', index=True)
    #                 df_alarmDetail.to_excel(writer, sheet_name='상세', index=True)
    #                 df_alarmExplain.to_excel(writer, sheet_name='설명', index=False)
    #         elif self.className == 'Fam3PowerThread':
    #             # 분류1 요약
    #             if len(df_alarmDetail) > 0:
    #                 df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
    #                 df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum()
    #                 df_firstAlarmSummary = df_firstAlarmSummary.reset_index()
    #                 df_firstAlarmSummary['수량'] = df_firstAlarmSummary['부족수량']
    #                 df_firstAlarmSummary['분류'] = '1'
    #                 df_firstAlarmSummary['MS CODE'] = '-'
    #                 df_firstAlarmSummary['검사호기'] = '-'
    #                 df_firstAlarmSummary['부족 시간'] = '-'
    #                 df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
    #                 del df_firstAlarmSummary['부족수량']
    #                 # 분류2-1 요약
    #                 df_secOneAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2-1']
    #                 df_secOneAlarmSummary = df_secOneAlarm.groupby("MS CODE")['부족수량'].sum()
    #                 df_secOneAlarmSummary = df_secOneAlarmSummary.reset_index()
    #                 df_secOneAlarmSummary['수량'] = df_secOneAlarmSummary['부족수량']
    #                 df_secOneAlarmSummary['분류'] = '2-1'
    #                 df_secOneAlarmSummary['SMT ASSY'] = '-'
    #                 df_secOneAlarmSummary['부족 시간'] = '-'
    #                 df_secOneAlarmSummary['Message'] = '당일 최소 필요생산 대수에 대하여 생산 불가능한 모델이 있습니다. 생산 허용비율을 확인해 주세요.'
    #                 del df_secOneAlarmSummary['부족수량']
    #                 # 분류2-2 요약
    #                 df_secTwoAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2-2']
    #                 df_secTwoAlarmSummary = df_secTwoAlarm.groupby("MS CODE")['부족수량'].sum()
    #                 df_secTwoAlarmSummary = df_secTwoAlarmSummary.reset_index()
    #                 df_secTwoAlarmSummary['수량'] = df_secTwoAlarmSummary['부족수량']
    #                 df_secTwoAlarmSummary['분류'] = '2-2'
    #                 df_secTwoAlarmSummary['SMT ASSY'] = '-'
    #                 df_secTwoAlarmSummary['부족 시간'] = '-'
    #                 df_secTwoAlarmSummary['Message'] = '당일 최소 필요생산 대수에 대하여 생산 불가능한 모델이 있습니다. 모델별 MAX 대수를 확인해 주세요.'
    #                 del df_secTwoAlarmSummary['부족수량']
    #                 # 분류 기타2 요약
    #                 df_etc2Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타2']
    #                 df_etc2AlarmSummary = df_etc2Alarm.groupby('MS CODE')['부족수량'].sum()
    #                 df_etc2AlarmSummary = df_etc2AlarmSummary.reset_index()
    #                 df_etc2AlarmSummary['수량'] = df_etc2AlarmSummary['부족수량']
    #                 df_etc2AlarmSummary['분류'] = '기타2'
    #                 df_etc2AlarmSummary['SMT ASSY'] = '-'
    #                 df_etc2AlarmSummary['검사호기'] = '-'
    #                 df_etc2AlarmSummary['부족 시간'] = '-'
    #                 df_etc2AlarmSummary['Message'] = '긴급오더 및 당일착공 대상의 총 착공량이 입력한 최대착공량보다 큽니다. 최대착공량을 확인해주세요.'
    #                 del df_etc2AlarmSummary['부족수량']
    #                 # 분류 기타4 요약
    #                 df_etc4Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타4']
    #                 df_etc4AlarmSummary = df_etc4Alarm.groupby('MS CODE')['부족수량'].sum()
    #                 df_etc4AlarmSummary = df_etc4AlarmSummary.reset_index()
    #                 df_etc4AlarmSummary['수량'] = df_etc4AlarmSummary['부족수량']
    #                 df_etc4AlarmSummary['분류'] = '기타4'
    #                 df_etc4AlarmSummary['SMT ASSY'] = '-'
    #                 df_etc4AlarmSummary['검사호기'] = '-'
    #                 df_etc4AlarmSummary['부족 시간'] = '-'
    #                 df_etc4AlarmSummary['Message'] = '설정된 CT 제한대수보다 최소 착공 필요량이 많습니다. 설정된 CT 제한대수를 확인해주세요.'
    #                 del df_etc4AlarmSummary['부족수량']
    #                 # 위 알람을 병합
    #                 df_alarmSummary = pd.concat([df_firstAlarmSummary, df_secOneAlarmSummary, df_secTwoAlarmSummary, df_etc2AlarmSummary, df_etc4AlarmSummary])
    #                 # 기타 알람에 대한 추가
    #                 df_etcList = df_alarmDetail[(df_alarmDetail['분류'] == '기타1') | (df_alarmDetail['분류'] == '기타3')]
    #                 df_etcList = df_etcList.drop_duplicates(['MS CODE'])
    #                 df_etcList = df_etcList.reset_index(drop=True)
    #                 for i in df_etcList.index:
    #                     if df_etcList['분류'][i] == '기타1':
    #                         df_alarmSummary = pd.concat([df_alarmSummary,
    #                                                     pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
    #                                                                                 "MS CODE": df_etcList['MS CODE'][i],
    #                                                                                 "SMT ASSY": '-',
    #                                                                                 "수량": 0,
    #                                                                                 "검사호기": '-',
    #                                                                                 "부족 시간": 0,
    #                                                                                 "Message": '해당 MS CODE에서 사용되는 SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'}])])
    #                     elif df_etcList['분류'][i] == '기타3':
    #                         df_alarmSummary = pd.concat([df_alarmSummary,
    #                                                     pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
    #                                                                                 "MS CODE": df_etcList['MS CODE'][i],
    #                                                                                 "SMT ASSY": '-',
    #                                                                                 "수량": 0,
    #                                                                                 "검사호기": '-',
    #                                                                                 "부족 시간": 0,
    #                                                                                 "Message": 'SMT ASSY 정보가 등록되지 않아 재고를 확인할 수 없습니다. 등록 후 다시 실행해주세요.'}])])
    #                 df_alarmSummary = df_alarmSummary.reset_index(drop=True)
    #                 df_alarmSummary = df_alarmSummary[['분류', 'MS CODE', 'SMT ASSY', '수량', '검사호기', '부족 시간', 'Message']]
    #                 if self.isDebug:
    #                     df_alarmSummary.to_excel('.\\debug\\FAM3\\Power\\df_alarmSummary.xlsx')
    #                 df_alarmExplain = pd.DataFrame({'분류': ['1', '2-1', '2-2', '기타1', '기타2', '기타3', '기타4'],
    #                                         '분류별 상황': ['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우',
    #                                         '당일 최소 착공필요량 > 모델별 생산 허용 비율인 경우',
    #                                         '당일 최소 착공필요량 > 모델별 MAX 대수인 경우',
    #                                         'MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우',
    #                                         '긴급오더 대상 착공시 최대착공량(사용자입력공수)이 부족할 경우',
    #                                         'SMT ASSY 정보가 DB에 미등록된 경우',
    #                                         '당일 최소 착공필요량 > CT제한 대수인 경우']})
    #                 # 파일 한개로 출력
    #                 if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}'):
    #                     os.makedirs(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}')
    #                 with pd.ExcelWriter(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}\\FAM3_AlarmList_{str(today)}_Power.xlsx') as writer:
    #                     df_alarmSummary.to_excel(writer, sheet_name='정리', index=True)
    #                     df_alarmDetail.to_excel(writer, sheet_name='상세', index=True)
    #                     df_alarmExplain.to_excel(writer, sheet_name='설명', index=False)
    #         elif self.className == 'Fam3SpThread':
    #             # 분류1 요약
    #             if len(df_alarmDetail) > 0:
    #                 df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
    #                 df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum()
    #                 df_firstAlarmSummary = df_firstAlarmSummary.reset_index()
    #                 df_firstAlarmSummary['분류'] = '1'
    #                 df_firstAlarmSummary['MS CODE'] = '-'
    #                 df_firstAlarmSummary['검사호기(그룹)'] = '-'
    #                 df_firstAlarmSummary['부족 시간'] = '-'
    #                 df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
    #                 df_secAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2']
    #                 df_secAlarmSummary = df_secAlarm.groupby("MS CODE")['부족수량'].max()
    #                 df_secAlarmSummary = pd.merge(df_secAlarmSummary, df_alarmDetail[['MS CODE', '검사호기(그룹)']], how='left', on='MS CODE').drop_duplicates('MS CODE')
    #                 df_secAlarmSummary = df_secAlarmSummary.reset_index()
    #                 df_secAlarmSummary['부족 시간'] = '-'
    #                 df_secAlarmSummary['분류'] = '2'
    #                 df_secAlarmSummary['SMT ASSY'] = '-'
    #                 df_secAlarmSummary['Message'] = '당일 최대 착공 제한 대수가 부족합니다. 설정 데이터를 확인해 주세요.'
    #                 # 분류 기타2 요약
    #                 df_etc2Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타2']
    #                 df_etc2AlarmSummary = df_etc2Alarm.groupby('MS CODE')['부족수량'].sum()
    #                 df_etc2AlarmSummary = df_etc2AlarmSummary.reset_index()
    #                 df_etc2AlarmSummary['수량'] = df_etc2AlarmSummary['부족수량']
    #                 df_etc2AlarmSummary['분류'] = '기타2'
    #                 df_etc2AlarmSummary['SMT ASSY'] = '-'
    #                 df_etc2AlarmSummary['검사호기'] = '-'
    #                 df_etc2AlarmSummary['부족 시간'] = '-'
    #                 df_etc2AlarmSummary['Message'] = '긴급오더 및 당일착공 대상의 총 착공량이 입력한 최대착공량보다 큽니다. 최대착공량을 확인해주세요.'
    #                 del df_etc2AlarmSummary['부족수량']
    #                 # 분류 기타4 요약
    #                 df_etc4Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타4']
    #                 df_etc4AlarmSummary = df_etc4Alarm.groupby('MS CODE')['부족수량'].sum()
    #                 df_etc4AlarmSummary = df_etc4AlarmSummary.reset_index()
    #                 df_etc4AlarmSummary['수량'] = df_etc4AlarmSummary['부족수량']
    #                 df_etc4AlarmSummary['분류'] = '기타4'
    #                 df_etc4AlarmSummary['SMT ASSY'] = '-'
    #                 df_etc4AlarmSummary['검사호기'] = '-'
    #                 df_etc4AlarmSummary['부족 시간'] = '-'
    #                 df_etc4AlarmSummary['Message'] = '설정된 CT 제한대수보다 최소 착공 필요량이 많습니다. 설정된 CT 제한대수를 확인해주세요.'
    #                 del df_etc4AlarmSummary['부족수량']
    #                 # 위 알람을 병합
    #                 df_alarmSummary = pd.concat([df_firstAlarmSummary, df_secAlarmSummary, df_etc2AlarmSummary, df_etc4AlarmSummary])
    #                 # 기타 알람에 대한 추가
    #                 df_etcList = df_alarmDetail[(df_alarmDetail['분류'] == '기타1') | (df_alarmDetail['분류'] == '기타3')]
    #                 df_etcList = df_etcList.drop_duplicates(['MS CODE', '분류'])
    #                 df_etcList = df_etcList.reset_index()
    #                 for i in df_etcList.index:
    #                     if df_etcList['분류'][i] == '기타1':
    #                         df_alarmSummary = pd.concat([df_alarmSummary,
    #                                                     pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
    #                                                                                 "MS CODE": df_etcList['MS CODE'][i],
    #                                                                                 "SMT ASSY": '-',
    #                                                                                 "부족수량": 0,
    #                                                                                 "검사호기(그룹)": '-',
    #                                                                                 "부족 시간": 0,
    #                                                                                 "Message": '해당 MS CODE에서 사용되는 SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'}])])
    #                     elif df_etcList['분류'][i] == '기타3':
    #                         df_alarmSummary = pd.concat([df_alarmSummary,
    #                                                     pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
    #                                                                                 "MS CODE": df_etcList['MS CODE'][i],
    #                                                                                 "SMT ASSY": df_etcList['SMT ASSY'][i],
    #                                                                                 "수량": 0,
    #                                                                                 "검사호기(그룹)": '-',
    #                                                                                 "부족 시간": 0,
    #                                                                                 "Message": 'SMT ASSY 정보가 등록되지 않아 재고를 확인할 수 없습니다. 등록 후 다시 실행해주세요.'}])])
    #                 df_alarmSummary = df_alarmSummary.reset_index(drop=True)
    #                 df_alarmSummary = df_alarmSummary[['분류', 'MS CODE', 'SMT ASSY', '부족수량', '검사호기(그룹)', '부족 시간', 'Message']]
    #                 if self.isDebug:
    #                     df_alarmSummary.to_excel('.\\debug\\FAM3\\Sp\\df_alarmSummary.xlsx')
    #                 if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}'):
    #                     os.makedirs(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}')
    #                 df_alarmExplain = pd.DataFrame({'분류': ['1', '2', '기타1', '기타2', '기타3', '기타4'],
    #                                                     '분류별 상황': ['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우',
    #                                                     '당일 착공분(or 긴급착공분)에 대해 MAX 대수가 부족할 경우',
    #                                                     'MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우',
    #                                                     '긴급오더 대상 착공시 최대착공량(사용자입력공수)이 부족할 경우',
    #                                                     'SMT ASSY 정보가 DB에 미등록된 경우',
    #                                                     '당일 최소 착공필요량 > CT제한 대수인 경우']})
    #                 # 파일 한개로 출력
    #                 with pd.ExcelWriter(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}\\FAM3_AlarmList_{today}_Sp.xlsx') as writer:
    #                     df_alarmSummary.to_excel(writer, sheet_name='정리', index=True)
    #                     df_alarmDetail.to_excel(writer, sheet_name='상세', index=True)
    #                     df_alarmExplain.to_excel(writer, sheet_name='설명', index=False)

    def concatAlarmDetail(self, df_target, no, category, df_data, index, combinedGroup, shortageCnt):
        """
        Args:
            df_target(DataFrame)    : 알람상세내역 DataFrame
            no(int)                 : 알람 번호
            category(str)           : 알람 분류
            df_data(DataFrame)      : 원본 DataFrame
            index(int)              : 원본 DataFrame의 인덱스
            combinedGroup(str)            : Smt Assy 이름
            shortageCnt(int)        : 부족 수량
        Return:
            return(DataFrame)       : 알람상세 Merge결과 DataFrame
            no + 1 (int)            : 증가된 알람 번호
        """
        df_result = pd.DataFrame({
            "No.": no,
            "분류": category,
            "L/N": df_data['Linkage Number'][index],
            "MS CODE": df_data['MS Code'][index],
            "수주수량": df_data['미착공수주잔'][index],
            "부족수량": shortageCnt,
            "대상 검사시간(초)": 0,
            "필요시간(초)": 0,
            "완성예정일": df_data['Planned Prod. Completion date'][index]
        }, index=[0])

        if category == '1' or (self.className == 'Fam3SpThread' and category == '2'):
            df_result["SMT ASSY"] = '-' if self.className == 'Fam3PowerThread' and '2-' in category else combinedGroup
            df_result["검사호기"] = '-' if self.className == 'Fam3PowerThread' and '2-' in category else ('-' if category == '1' else combinedGroup)
            df_result["검사호기(그룹)"] = combinedGroup if self.className == 'Fam3SpThread' and category == '2' else '-'

        elif self.className == 'Fam3PowerThread' and '2-' in category:
            df_result["SMT ASSY"] = '-'
            df_result["검사호기"] = '-'

        elif category == '2' and self.className == 'Fam3MainThread':
            df_result["SMT ASSY"] = '-'
            df_result["검사호기"] = df_data['INSPECTION_EQUIPMENT'][index]
            df_result["대상 검사시간(초)"] = df_data['TotalTime'][index]
            df_result["필요시간(초)"] = shortageCnt * df_data['TotalTime'][index]

        elif category == '2' and self.className == 'JuxtaOldThread':
            df_result["SMT ASSY"] = '-'
            df_result["검사호기(그룹)"] = combinedGroup

        elif category == '기타1' or category == '기타2':
            smt_assy_value = '-' if category == '기타2' else '미등록'
            inspection_group = '-' if category == '기타1' else combinedGroup
            df_new = pd.DataFrame.from_records([{
                "No.": no,
                "분류": category,
                "L/N": df_data['Linkage Number'][index],
                "MS CODE": df_data['MS Code'][index],
                "SMT ASSY": smt_assy_value,
                "수주수량": df_data['미착공수주잔'][index],
                "부족수량": 0 if category == '기타1' else shortageCnt,
                "검사호기": '-' if category == '기타1' else '-',
                "검사호기(그룹)": inspection_group,
                "대상 검사시간(초)": 0,
                "필요시간(초)": 0,
                "완성예정일": df_data['Planned Prod. Completion date'][index]
            }])
            df_result = pd.concat([df_target, df_new])
        elif self.className == 'Fam3PowerThread' and category == '기타3':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": combinedGroup,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": 0,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif self.className == 'Fam3SpThread' and category == '기타3':
            df_result = pd.concat([df_target,
                        pd.DataFrame.from_records([{"No.": no,
                                                    "분류": category,
                                                    "L/N": df_data['Linkage Number'][index],
                                                    "MS CODE": df_data['MS Code'][index],
                                                    "SMT ASSY": combinedGroup,
                                                    "수주수량": df_data['미착공수주잔'][index],
                                                    "부족수량": 0,
                                                    "검사호기(그룹)": '-',
                                                    "대상 검사시간(초)": 0,
                                                    "필요시간(초)": 0,
                                                    "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif category == '기타3':
            df_result = pd.concat([df_target,
                                    pd.DataFrame.from_records([{"No.": no,
                                                                "분류": category,
                                                                "L/N": df_data['Linkage Number'][index],
                                                                "MS CODE": df_data['MS Code'][index],
                                                                "SMT ASSY": combinedGroup,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        elif self.className == 'Fam3SpThread' and category == '기타4':
            df_result = pd.concat([df_target,
                                pd.DataFrame.from_records([{"No.": no,
                                                            "분류": category,
                                                            "L/N": df_data['Linkage Number'][index],
                                                            "MS CODE": df_data['MS Code'][index],
                                                            "SMT ASSY": combinedGroup,
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
                                                                "SMT ASSY": combinedGroup,
                                                                "수주수량": df_data['미착공수주잔'][index],
                                                                "부족수량": shortageCnt,
                                                                "검사호기": '-',
                                                                "대상 검사시간(초)": 0,
                                                                "필요시간(초)": 0,
                                                                "완성예정일": df_data['Planned Prod. Completion date'][index]}])])
        return [df_result, no + 1]

    def summary(self, df_alarmDetail):
        today = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            today = self.date
        if len(df_alarmDetail) > 0:
            df_alarmSummary = pd.DataFrame()

            if self.className == 'Fam3MainThread':
                df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
                df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum().reset_index()
                df_firstAlarmSummary['수량'] = df_firstAlarmSummary['부족수량']
                df_firstAlarmSummary['분류'] = '1'
                df_firstAlarmSummary['MS CODE'] = '-'
                df_firstAlarmSummary['검사호기'] = '-'
                df_firstAlarmSummary['부족 시간'] = '-'
                df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
                del df_firstAlarmSummary['부족수량']
                df_alarmSummary = pd.concat([df_alarmSummary, df_firstAlarmSummary])

                df_secAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2']
                df_secAlarmSummary = df_secAlarm.groupby("검사호기")['필요시간(초)'].sum().reset_index()
                df_secAlarmSummary['부족 시간'] = df_secAlarmSummary['필요시간(초)'].apply(self.convertSecToTime)
                df_secAlarmSummary['분류'] = '2'
                df_secAlarmSummary['MS CODE'] = '-'
                df_secAlarmSummary['SMT ASSY'] = '-'
                df_secAlarmSummary['수량'] = '-'
                df_secAlarmSummary['Message'] = '검사설비능력이 부족합니다. 생산 가능여부를 확인해 주세요.'
                del df_secAlarmSummary['필요시간(초)']
                df_alarmSummary = pd.concat([df_alarmSummary, df_secAlarmSummary])

            elif self.className == 'Fam3PowerThread':
                df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
                df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum().reset_index()
                df_firstAlarmSummary['수량'] = df_firstAlarmSummary['부족수량']
                df_firstAlarmSummary['분류'] = '1'
                df_firstAlarmSummary['MS CODE'] = '-'
                df_firstAlarmSummary['검사호기'] = '-'
                df_firstAlarmSummary['부족 시간'] = '-'
                df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
                del df_firstAlarmSummary['부족수량']
                df_alarmSummary = pd.concat([df_alarmSummary, df_firstAlarmSummary])

                df_secOneAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2-1']
                df_secOneAlarmSummary = df_secOneAlarm.groupby("MS CODE")['부족수량'].sum().reset_index()
                df_secOneAlarmSummary['수량'] = df_secOneAlarmSummary['부족수량']
                df_secOneAlarmSummary['분류'] = '2-1'
                df_secOneAlarmSummary['SMT ASSY'] = '-'
                df_secOneAlarmSummary['부족 시간'] = '-'
                df_secOneAlarmSummary['Message'] = '당일 최소 필요생산 대수에 대하여 생산 불가능한 모델이 있습니다. 생산 허용비율을 확인해 주세요.'
                del df_secOneAlarmSummary['부족수량']
                df_alarmSummary = pd.concat([df_alarmSummary, df_secOneAlarmSummary])

                df_secTwoAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2-2']
                df_secTwoAlarmSummary = df_secTwoAlarm.groupby("MS CODE")['부족수량'].sum().reset_index()
                df_secTwoAlarmSummary['수량'] = df_secTwoAlarmSummary['부족수량']
                df_secTwoAlarmSummary['분류'] = '2-2'
                df_secTwoAlarmSummary['SMT ASSY'] = '-'
                df_secTwoAlarmSummary['부족 시간'] = '-'
                df_secTwoAlarmSummary['Message'] = '당일 최소 필요생산 대수에 대하여 생산 불가능한 모델이 있습니다. 모델별 MAX 대수를 확인해 주세요.'
                del df_secTwoAlarmSummary['부족수량']
                df_alarmSummary = pd.concat([df_alarmSummary, df_secTwoAlarmSummary])

            elif self.className == 'Fam3SpThread':
                df_firstAlarm = df_alarmDetail[df_alarmDetail['분류'] == '1']
                df_firstAlarmSummary = df_firstAlarm.groupby("SMT ASSY")['부족수량'].sum().reset_index()
                df_firstAlarmSummary['분류'] = '1'
                df_firstAlarmSummary['MS CODE'] = '-'
                df_firstAlarmSummary['검사호기(그룹)'] = '-'
                df_firstAlarmSummary['부족 시간'] = '-'
                df_firstAlarmSummary['Message'] = '[SMT ASSY : ' + df_firstAlarmSummary["SMT ASSY"] + ']가 부족합니다. SMT ASSY 제작을 지시해주세요.'
                df_alarmSummary = pd.concat([df_alarmSummary, df_firstAlarmSummary])

                df_secAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2']
                df_secAlarmSummary = df_secAlarm.groupby("MS CODE")['부족수량'].max().reset_index()
                df_secAlarmSummary = pd.merge(df_secAlarmSummary, df_alarmDetail[['MS CODE', '검사호기(그룹)']], how='left', on='MS CODE').drop_duplicates('MS CODE')
                df_secAlarmSummary['부족 시간'] = '-'
                df_secAlarmSummary['분류'] = '2'
                df_secAlarmSummary['SMT ASSY'] = '-'
                df_secAlarmSummary['Message'] = '당일 최대 착공 제한 대수가 부족합니다. 설정 데이터를 확인해 주세요.'
                df_alarmSummary = pd.concat([df_alarmSummary, df_secAlarmSummary])
                df_etc2Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타2']
                df_etc2AlarmSummary = df_etc2Alarm.groupby('MS CODE')['부족수량'].sum().reset_index()
                df_etc2AlarmSummary['수량'] = df_etc2AlarmSummary['부족수량']
                df_etc2AlarmSummary['분류'] = '기타2'
                df_etc2AlarmSummary['SMT ASSY'] = '-'
                df_etc2AlarmSummary['검사호기'] = '-'
                df_etc2AlarmSummary['부족 시간'] = '-'
                df_etc2AlarmSummary['Message'] = '긴급오더 및 당일착공 대상의 총 착공량이 입력한 최대착공량보다 큽니다. 최대착공량을 확인해주세요.'
                del df_etc2AlarmSummary['부족수량']
                df_alarmSummary = pd.concat([df_alarmSummary, df_etc2AlarmSummary])

            elif self.className == 'JuxtaOldThread':
                df_secAlarm = df_alarmDetail[df_alarmDetail['분류'] == '2']
                df_secAlarmSummary = df_secAlarm.groupby("MS CODE")['부족수량'].max().reset_index()
                df_secAlarmSummary = pd.merge(df_secAlarmSummary, df_alarmDetail[['MS CODE', '검사호기(그룹)']], how='left', on='MS CODE').drop_duplicates('MS CODE')
                df_secAlarmSummary['부족 시간'] = '-'
                df_secAlarmSummary['분류'] = '2'
                df_secAlarmSummary['SMT ASSY'] = '-'
                df_secAlarmSummary['Message'] = '당일 최대 착공 제한 대수가 부족합니다. 설정 데이터를 확인해 주세요.'
                df_alarmSummary = pd.concat([df_secAlarmSummary])
                print(df_alarmSummary)

            df_etc4Alarm = df_alarmDetail[df_alarmDetail['분류'] == '기타4']
            df_etc4AlarmSummary = df_etc4Alarm.groupby('MS CODE')['부족수량'].sum().reset_index()
            df_etc4AlarmSummary['수량'] = df_etc4AlarmSummary['부족수량']
            df_etc4AlarmSummary['분류'] = '기타4'
            df_etc4AlarmSummary['SMT ASSY'] = '-'
            df_etc4AlarmSummary['검사호기'] = '-'
            df_etc4AlarmSummary['부족 시간'] = '-'
            df_etc4AlarmSummary['Message'] = '설정된 CT 제한대수보다 최소 착공 필요량이 많습니다. 설정된 CT 제한대수를 확인해주세요.'
            del df_etc4AlarmSummary['부족수량']
            df_alarmSummary = pd.concat([df_alarmSummary, df_etc4AlarmSummary])

            df_etcList = df_alarmDetail[(df_alarmDetail['분류'] == '기타1') | (df_alarmDetail['분류'] == '기타3')]
            df_etcList = df_etcList.drop_duplicates(['MS CODE', '분류'])
            df_etcList = df_etcList.reset_index()
            for i in df_etcList.index:
                if df_etcList['분류'][i] == '기타1':
                    df_alarmSummary = pd.concat([df_alarmSummary,
                                                pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                            "MS CODE": df_etcList['MS CODE'][i],
                                                                            "SMT ASSY": '-',
                                                                            "부족수량": 0,
                                                                            "검사호기(그룹)": '-',
                                                                            "부족 시간": 0,
                                                                            "Message": '해당 MS CODE에서 사용되는 SMT ASSY가 등록되지 않았습니다. 등록 후 다시 실행해주세요.'}])])
                elif df_etcList['분류'][i] == '기타3':
                    df_alarmSummary = pd.concat([df_alarmSummary,
                                            pd.DataFrame.from_records([{"분류": df_etcList['분류'][i],
                                                                        "MS CODE": df_etcList['MS CODE'][i],
                                                                        "SMT ASSY": df_etcList['SMT ASSY'][i],
                                                                        "수량": 0,
                                                                        "검사호기(그룹)": '-',
                                                                        "부족 시간": 0,
                                                                        "Message": 'SMT ASSY 정보가 등록되지 않아 재고를 확인할 수 없습니다. 등록 후 다시 실행해주세요.'}])])

            df_alarmSummary = df_alarmSummary.reset_index(drop=True)
            df_alarmSummary = df_alarmSummary[['분류', 'MS CODE', 'SMT ASSY', '부족수량', '검사호기(그룹)', '부족 시간', 'Message']]
            df_alarmExplain = pd.DataFrame({'분류': ['1', '2', '기타1', '기타2', '기타3', '기타4'],
                                            '분류별 상황': ['DB상의 Smt Assy가 부족하여 해당 MS-Code를 착공 내릴 수 없는 경우',
                                                        '당일 착공분(or 긴급착공분)에 대해 MAX 대수가 부족할 경우',
                                                        'MS-Code와 일치하는 Smt Assy가 마스터 파일에 없는 경우',
                                                        '긴급오더 대상 착공시 최대착공량(사용자입력공수)이 부족할 경우',
                                                        'SMT ASSY 정보가 DB에 미등록된 경우',
                                                        '당일 최소 착공필요량 > CT제한 대수인 경우']})
            if self.className == 'Fam3MainThread':
                path = f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}\\FAM3_AlarmList_{today}_Main.xlsx'
            elif self.className == 'Fam3SpThread':
                path = f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}\\FAM3_AlarmList_{today}_Sp.xlsx'
            elif self.className == 'Fam3PowerThread':
                path = f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}\\FAM3_AlarmList_{today}_Power.xlsx'
            elif self.className == 'JuxtaOldThread':
                path = f'.\\Result\\JUXTA\\{str(today)}\\Alarm\\JUXTA_AlarmList_{today}_Old.xlsx'

            if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}'):
                os.makedirs(f'.\\Result\\FAM3\\{str(today)}\\Alarm\\{self.cb_round}')
            if not os.path.exists(f'.\\Result\\JUXTA\\{str(today)}\\Alarm\\'):
                os.makedirs(f'.\\Result\\JUXTA\\{str(today)}\\Alarm\\')

            with pd.ExcelWriter(path) as writer:
                df_alarmSummary.to_excel(writer, sheet_name='정리', index=True)
                df_alarmDetail.to_excel(writer, sheet_name='상세', index=True)
                df_alarmExplain.to_excel(writer, sheet_name='설명', index=False)