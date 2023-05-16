import pandas as pd
import datetime
import os
import numpy as np


class Cycling():
    def __init__(self, productClass=None):
        super().__init__()
        self.isDebug = productClass.isDebug
        self.className = type(productClass).__name__
        self.emgHoldList = productClass.emgHoldList
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SPThread':
            self.cb_round = productClass.cb_round               # Round callback function
        else:
            self.cb_round = ''
        if self.className == 'Fam3MainThread' or self.className == 'Fam3PowerThread' or self.className == 'Fam3SPThread':
            self.firstModCnt = productClass.moduleMaxCnt
        elif self.className == 'JuxtaOldThread':
            self.firstModCnt = productClass.firstOrdCnt
        if self.className == 'Fam3SpThread':
            self.secondModCnt = productClass.nonModuleMaxCnt
        elif self.className == 'JuxtaOldThread':
            self.secondModCnt = productClass.secondOrdCnt
        self.list_masterFile = productClass.list_masterFile
        self.constDate = productClass.constDate
        self.date = productClass.date

    def excute(self, df, df_leveling, maxNo, maxNoBL, maxNoTerminal, maxNoSlave):
        today = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            today = self.date
        emgLinkage = self.emgHoldList[0]
        emgmscode = self.emgHoldList[1]
        # 긴급오더, 홀딩오더 데이터프레임화
        df_emgLinkage = pd.DataFrame({'Linkage Number': emgLinkage})
        df_emgmscode = pd.DataFrame({'MS Code': emgmscode})
        df_emgLinkage['Linkage Number'] = df_emgLinkage['Linkage Number'].astype(str)
        if self.className == 'Fam3MainThread':
            # 레벨링 리스트와 병합
            df = df.astype({'Linkage Number': 'str'})
            df_leveling = df_leveling.astype({'Linkage Number': 'str'})
            df_mergeOrder = pd.merge(df, df_leveling, on='Linkage Number', how='left')
            df_mergeOrderResult = pd.DataFrame().reindex_like(df_mergeOrder)
            df_mergeOrderResult = df_mergeOrderResult[0:0]
            # 총착공량 만큼 개별화
            for i in df.index:
                for j in df_mergeOrder.index:
                    if df['Linkage Number'][i] == df_mergeOrder['Linkage Number'][j]:
                        if j > 0:
                            if df_mergeOrder['Linkage Number'][j] != df_mergeOrder['Linkage Number'][j - 1]:
                                orderCnt = int(df['총착공량'][i])
                        else:
                            orderCnt = int(df['총착공량'][i])
                        if orderCnt > 0:
                            df_mergeOrderResult = df_mergeOrderResult.append(df_mergeOrder.iloc[j])
                            orderCnt -= 1
            # 사이클링을 위해 검사설비별로 정리
            df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['INSPECTION_EQUIPMENT'], ascending=[False])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
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

            # self.mainReturnEmgLinkage.emit(dict_emgLinkage)
            # self.mainReturnEmgMscode.emit(dict_emgMscode)
            if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}'):
                os.makedirs(f'.\\Result\\FAM3\\{str(today)}')
            if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}'):
                os.makedirs(f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}')
            outputFile = f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}\\{str(today)}_Main.xlsx'
            df_mergeOrderResult.to_excel(outputFile, index=False)
        elif self.className == 'Fam3PowerThread':
            # 레벨링 리스트와 병합
            df = df.astype({'Linkage Number': 'str'})
            df_leveling = df_leveling.astype({'Linkage Number': 'str'})
            df_mergeOrder = pd.merge(df, df_leveling, on='Linkage Number', how='inner')
            df_mergeOrderResult = pd.DataFrame().reindex_like(df_mergeOrder)
            df_mergeOrderResult = df_mergeOrderResult[0:0]
            # 총착공량 만큼 개별화
            for i in df.index:
                for j in df_mergeOrder.index:
                    if df['Linkage Number'][i] == df_mergeOrder['Linkage Number'][j]:
                        if j > 0:
                            if df_mergeOrder['Linkage Number'][j] != df_mergeOrder['Linkage Number'][j - 1]:
                                orderCnt = int(df['총착공량'][i])
                        else:
                            orderCnt = int(df['총착공량'][i])
                        if orderCnt > 0:
                            df_mergeOrderResult = df_mergeOrderResult.append(df_mergeOrder.iloc[j])
                            orderCnt -= 1
            # 사이클링을 위해 검사설비별로 정리
            df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['MODEL'], ascending=[False])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
            df_unCt = df_mergeOrderResult[df_mergeOrderResult['MS Code'].str.contains('/CT')]
            df_mergeOrderResult = df_mergeOrderResult[~df_mergeOrderResult['MS Code'].str.contains('/CT')]
            # 긴급오더 제외하고 사이클 대상만 식별하여 검사장치별로 갯수 체크
            df_cycleCopy = df_mergeOrderResult[df_mergeOrderResult['긴급오더'].isnull()]
            df_cycleBuForward = df_cycleCopy[df_cycleCopy['상세구분'] == 'BASE']
            df_cycleBuForward = df_cycleCopy[df_cycleCopy['상세구분'] == 'BASE'].sort_values(by=['MODEL'], ascending=[True])
            df_cycleBuForward = df_cycleBuForward.reset_index(drop=True)
            df_cycleBuBack = df_cycleCopy[df_cycleCopy['상세구분'] == 'BASE'].sort_values(by=['MODEL'], ascending=[False])
            df_cycleBuBack = df_cycleBuBack.reset_index(drop=True)
            df_cyclePuForward = df_cycleCopy[df_cycleCopy['상세구분'] == 'POWER']
            df_cyclePuForward = df_cycleCopy[df_cycleCopy['상세구분'] == 'POWER'].sort_values(by=['MODEL'], ascending=[True])
            df_cyclePuForward = df_cyclePuForward.reset_index(drop=True)
            df_cyclePuBack = df_cycleCopy[df_cycleCopy['상세구분'] == 'POWER'].sort_values(by=['MODEL'], ascending=[False])
            df_cyclePuBack = df_cyclePuBack.reset_index(drop=True)
            df_cycleBuCopy = pd.DataFrame(columns=df_cycleCopy.columns)
            df_cyclePuCopy = pd.DataFrame(columns=df_cycleCopy.columns)
            # BU/PU 구분지어 별도 파일을 생성. 사양을 지그재그방식으로 순서를 재조정한다. (생산요청사항)
            for i in df_cycleBuForward.index:
                df_dupCheck = df_cycleBuCopy['Serial Number'].str.contains(df_cycleBuForward['Serial Number'][i]).sum()
                if len(df_cycleBuForward) > len(df_cycleBuCopy):
                    if df_dupCheck == 0:
                        df_cycleBuCopy = df_cycleBuCopy.append(df_cycleBuForward.iloc[i])
                else:
                    break
                df_dupCheck = df_cycleBuCopy['Serial Number'].str.contains(df_cycleBuBack['Serial Number'][i]).sum()
                if len(df_cycleBuForward) > len(df_cycleBuCopy):
                    if df_dupCheck == 0:
                        df_cycleBuCopy = df_cycleBuCopy.append(df_cycleBuBack.iloc[i])
                else:
                    break
            for i in df_cyclePuForward.index:
                df_dupCheck = df_cyclePuCopy['Serial Number'].str.contains(df_cyclePuForward['Serial Number'][i]).sum()
                if len(df_cyclePuForward) > len(df_cyclePuCopy):
                    if df_dupCheck == 0:
                        df_cyclePuCopy = df_cyclePuCopy.append(df_cyclePuForward.iloc[i])
                else:
                    break
                df_dupCheck = df_cyclePuCopy['Serial Number'].str.contains(df_cyclePuBack['Serial Number'][i]).sum()
                if len(df_cyclePuForward) > len(df_cyclePuCopy):
                    if df_dupCheck == 0:
                        df_cyclePuCopy = df_cyclePuCopy.append(df_cyclePuBack.iloc[i])
                else:
                    break
            df_cycleBuCopy = df_cycleBuCopy.reset_index()
            df_cyclePuCopy = df_cyclePuCopy.reset_index()
            # 분리했던 BU/PU 파일을 하나로 병합
            df_cycleCopy = pd.concat([df_cycleBuCopy, df_cyclePuCopy])
            df_cycleCopy['ModelCnt'] = df_cycleCopy.groupby('상세구분')['상세구분'].transform('size')
            df_cycleCopy = df_cycleCopy.sort_values(by=['ModelCnt', 'index'], ascending=[False, True])
            df_cycleCopy = df_cycleCopy.reset_index(drop=True)
            # 긴급오더 포함한 Df와 병합
            # df_mergeOrderResult = pd.merge(df_mergeOrderResult, df_cycleCopy[['Planned Order', 'ModelCnt']], on='Planned Order', how='left')
            df_mergeOrderResult = pd.concat([df_mergeOrderResult[df_mergeOrderResult['긴급오더'] == '대상'], df_cycleCopy])
            df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['ModelCnt', 'index'], ascending=[False, True])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
            # 최대 사이클 번호 체크
            maxCycle = float(df_cycleCopy['ModelCnt'][0])
            cycleGr = 1.0
            df_mergeOrderResult['사이클그룹'] = 0
            # 각 검사장치별로 사이클 그룹을 작성하고, 최대 사이클과 비교하여 각 사이클그룹에서 배수처리
            for i in df_mergeOrderResult.index:
                if df_mergeOrderResult['긴급오더'][i] != '대상':
                    multiCnt = maxCycle / df_mergeOrderResult['ModelCnt'][i]
                    if i == 0:
                        df_mergeOrderResult['사이클그룹'][i] = cycleGr
                    else:
                        if df_mergeOrderResult['상세구분'][i] != df_mergeOrderResult['상세구분'][i - 1]:
                            if i == 1:
                                cycleGr = 2.0
                            else:
                                cycleGr = 1.0
                        df_mergeOrderResult['사이클그룹'][i] = cycleGr * multiCnt
                    cycleGr += 1.0
                if cycleGr >= maxCycle:
                    cycleGr = 1.0
            # 배정된 사이클 그룹 순으로 정렬
            df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['사이클그룹', 'index'], ascending=[True, True])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
            df_mergeOrderResult = df_mergeOrderResult.reset_index()
            # 연속으로 같은 검사설비가 오지 않도록 순서를 재조정
            for i in df_mergeOrderResult.index:
                if df_mergeOrderResult['긴급오더'][i] != '대상':
                    if (i != 0 and (df_mergeOrderResult['상세구분'][i] == df_mergeOrderResult['상세구분'][i - 1])):
                        for j in df_mergeOrderResult.index:
                            if df_mergeOrderResult['긴급오더'][j] != '대상':
                                if ((j != 0 and j < len(df_mergeOrderResult) - 1) and (df_mergeOrderResult['상세구분'][i] != df_mergeOrderResult['상세구분'][j + 1]) and (df_mergeOrderResult['상세구분'][i] != df_mergeOrderResult['상세구분'][j])):
                                    df_mergeOrderResult['level_0'][i] = (float(df_mergeOrderResult['level_0'][j]) + float(df_mergeOrderResult['level_0'][j + 1])) / 2
                                    df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['level_0'], ascending=[True])
                                    df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                                    break

            df_unCt['index'] = 0
            df_unCt['사이클그룹'] = 0
            df_mergeOrderResult = pd.concat([df_unCt, df_mergeOrderResult])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
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

            # self.powerReturnEmgLinkage.emit(dict_emgLinkage)
            # self.powerReturnEmgMscode.emit(dict_emgMscode)

            if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}'):
                os.makedirs(f'.\\Result\\FAM3\\{str(today)}')
            if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}'):
                os.makedirs(f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}')
            outputFile = f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}\\{str(today)}_Power.xlsx'
            df_mergeOrderResult.to_excel(outputFile, index=False)
        elif self.className == 'Fam3SpThread':
            # 레벨링 리스트와 병합
            df = df.astype({'Linkage Number': 'str'})
            df_leveling = df_leveling.astype({'Linkage Number': 'str'})
            df_mergeOrder = pd.merge(df, df_leveling, on='Linkage Number', how='left')
            df_mergeOrderResult = pd.DataFrame().reindex_like(df_mergeOrder)
            df_mergeOrderResult = df_mergeOrderResult[0:0]
            # 총착공량 만큼 개별화
            for i in df.index:
                for j in df_mergeOrder.index:
                    if df['Linkage Number'][i] == df_mergeOrder['Linkage Number'][j]:
                        if j > 0:
                            if df_mergeOrder['Linkage Number'][j] != df_mergeOrder['Linkage Number'][j - 1]:
                                orderCnt = int(df['총착공량'][i])
                        else:
                            orderCnt = int(df['총착공량'][i])
                        if orderCnt > 0:
                            df_mergeOrderResult = df_mergeOrderResult.append(df_mergeOrder.iloc[j])
                            orderCnt -= 1
            # 사이클링을 위해 검사설비별로 정리
            df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['대표모델'], ascending=[False])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
            # 긴급오더 제외하고 사이클 대상만 식별하여 검사장치별로 갯수 체크
            if len(df_mergeOrderResult) > 0:
                df_unCt = df_mergeOrderResult[df_mergeOrderResult['MS Code'].str.contains('/CT')]
                df_mergeOrderResult = df_mergeOrderResult[~df_mergeOrderResult['MS Code'].str.contains('/CT')]
                df_cycleCopy = df_mergeOrderResult[df_mergeOrderResult['긴급오더'].isnull()]
                df_cycleCopy['대표모델'] = df_cycleCopy['MS Code'].str[:9]
                df_cycleCopy['대표모델Cnt'] = df_cycleCopy.groupby('대표모델')['대표모델'].transform('size')
                df_cycleCopy = df_cycleCopy.sort_values(by=['대표모델Cnt'], ascending=[False])
                df_cycleCopy = df_cycleCopy.reset_index(drop=True)
                # 긴급오더 포함한 Df와 병합
                df_mergeOrderResult = pd.merge(df_mergeOrderResult, df_cycleCopy[['Planned Order', '대표모델Cnt']], on='Planned Order', how='left')
                df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['대표모델Cnt'], ascending=[False])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                df_module = df_mergeOrderResult[df_mergeOrderResult['모듈 구분_x'] == '모듈']
                df_module = df_module[~df_module['MODEL'].str.contains('TAH')]
                df_module = df_module.reset_index(drop=True)
                if self.cb_round == '1차':
                    df_slave = df_mergeOrderResult[df_mergeOrderResult['MODEL'].str.contains('TAH')]
                    df_slave = df_slave.reset_index(drop=True)
                if self.cb_round == '2차':
                    df_BL = df_mergeOrderResult[df_mergeOrderResult['MODEL'].str.contains('F3BL00')]
                    df_BL = df_BL.reset_index(drop=True)
                    df_terminal = df_mergeOrderResult[df_mergeOrderResult['MODEL'].str.contains('RK|TA40')]
                    df_terminal = df_terminal.reset_index(drop=True)
                # 최대 사이클 번호 체크
                maxCycle = float(df_cycleCopy['대표모델Cnt'][0])
                cycleGr = 1.0
                df_module['사이클그룹'] = 0
                # 각 검사장치별로 사이클 그룹을 작성하고, 최대 사이클과 비교하여 각 사이클그룹에서 배수처리
                for i in df_module.index:
                    if df_module['긴급오더'][i] != '대상':
                        multiCnt = maxCycle / df_module['대표모델Cnt'][i]
                        if i == 0:
                            df_module['사이클그룹'][i] = cycleGr
                        else:
                            if df_module['대표모델'][i] != df_module['대표모델'][i - 1]:
                                if i == 1:
                                    cycleGr = 2.0
                                else:
                                    cycleGr = 1.0
                            df_module['사이클그룹'][i] = cycleGr * multiCnt
                        cycleGr += 1.0
                    if cycleGr >= maxCycle:
                        cycleGr = 1.0
                # 배정된 사이클 그룹 순으로 정렬
                df_module = df_module.sort_values(by=['사이클그룹'], ascending=[True])
                df_module = df_module.reset_index(drop=True)
                df_module = df_module.reset_index()
                for i in df_module.index:
                    if df_module['긴급오더'][i] != '대상':
                        if (i != 0 and (df_module['대표모델'][i] == df_module['대표모델'][i - 1])):
                            for j in df_module.index:
                                if df_module['긴급오더'][j] != '대상':
                                    if ((j != 0 and j < len(df_module) - 1) and (df_module['대표모델'][i] != df_module['대표모델'][j + 1]) and (df_module['대표모델'][i] != df_module['대표모델'][j])):
                                        df_module['index'][i] = ((float(df_module['index'][j]) + float(df_module['index'][j + 1])) / 2)
                                        df_module = df_module.sort_values(by=['index'], ascending=[True])
                                        df_module = df_module.reset_index(drop=True)
                                        break
                df_unCt['index'] = 0
                df_unCt['사이클그룹'] = 0
                # CT사양과 병합
                df_module = pd.concat([df_unCt, df_module])
                df_module = df_module.reset_index(drop=True)
                df_module['No (*)'] = int(maxNo) + (df_module.index.astype(int) + 1) * 10
                # 1차 착공일 경우, 슬레이브에 대한 결과를 만들고 병합
                if self.cb_round == '1차':
                    if len(df_slave) > 0:
                        df_slave = df_slave.reset_index(drop=True)
                        df_slave['No (*)'] = int(maxNoSlave) + (df_slave.index.astype(int) + 1) * 10
                        if self.isDebug:
                            df_slave.to_excel('.\\debug\\FAM3\\Sp\\df_slave.xlsx')
                    df_mergeOrderResult = pd.concat([df_module, df_slave])
                # 2차 착공일 경우, 베이스와 터미널에 대한 결과를 만들고 병합
                if self.cb_round == '2차':
                    if len(df_BL) > 0:
                        df_BL = df_BL.reset_index(drop=True)
                        df_BL['No (*)'] = int(maxNoBL) + (df_BL.index.astype(int) + 1) * 10
                    if len(df_terminal) > 0:
                        df_terminal = df_terminal.sort_values(by=['MODEL'], ascending=[True])
                        df_terminal = df_terminal.reset_index(drop=True)
                        df_terminal['No (*)'] = int(maxNoTerminal) + (df_terminal.index.astype(int) + 1) * 10
                    if self.isDebug:
                        df_BL.to_excel('.\\debug\\FAM3\\Sp\\df_BL.xlsx')
                        df_terminal.to_excel('.\\debug\\FAM3\\Sp\\df_terminal.xlsx')
                    df_mergeOrderResult = pd.concat([df_module, df_BL, df_terminal])
                df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
                # df_mergeOrderResult['No (*)'] = (df_mergeOrderResult.index.astype(int) + 1) * 10
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

                # self.spReturnEmgLinkage.emit(dict_emgLinkage)
                # self.spReturnEmgMscode.emit(dict_emgMscode)
                if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}'):
                    os.makedirs(f'.\\Result\\FAM3\\{str(today)}')
                if not os.path.exists(f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}'):
                    os.makedirs(f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}')
                outputFile = f'.\\Result\\FAM3\\{str(today)}\\{self.cb_round}\\{str(today)}_Sp.xlsx'
                df_mergeOrderResult.to_excel(outputFile, index=False)
        elif self.className == 'JuxtaOldThread':
            # 레벨링 리스트와 병합
            df = df.astype({'Linkage Number': 'str'})
            df_leveling = df_leveling.astype({'Linkage Number': 'str'})
            df_mergeOrder = pd.merge(df, df_leveling, on='Linkage Number', how='left')
            df_mergeOrderResult = pd.DataFrame().reindex_like(df_mergeOrder)
            df_mergeOrderResult = df_mergeOrderResult[0:0]
            # 총착공량 만큼 개별화
            for i in df.index:
                for j in df_mergeOrder.index:
                    if df['Linkage Number'][i] == df_mergeOrder['Linkage Number'][j]:
                        if j > 0:
                            if df_mergeOrder['Linkage Number'][j] != df_mergeOrder['Linkage Number'][j - 1]:
                                orderCnt = int(df['총착공량'][i])
                        else:
                            orderCnt = int(df['총착공량'][i])
                        if orderCnt > 0:
                            df_mergeOrderResult = df_mergeOrderResult.append(df_mergeOrder.iloc[j])
                            orderCnt -= 1

            df_cycleCond = pd.read_excel(self.list_masterFile[3], sheet_name=3, skiprows=2)
            df_cycleCond.columns = ['No', '사이클 그룹명', 'FullCode(자동완성)', '1', '2', '3', '4', '5', '6', 'OptionCode', '사이클 실행여부', '생산기준대수', '우선순위', '후순위']
            df_cycleCond[['No', '사이클 그룹명']] = df_cycleCond[['No', '사이클 그룹명']].fillna(method='ffill')
            df_cycleCond[['사이클 실행여부', '생산기준대수', '우선순위', '후순위']] = df_cycleCond[['사이클 실행여부', '생산기준대수', '우선순위', '후순위']].fillna(method='ffill')
            dict_cycleNo = {}

            df_mergeOrderResult['사이클 그룹'] = ''
            df_mergeOrderResult['우선순위'] = ''
            df_mergeOrderResult['후순위'] = ''

            for i, merge_order_row in df_mergeOrderResult.iterrows():
                for j, cycle_row in df_cycleCond.iterrows():
                    dict_cycleNo[str(cycle_row['No'])] = [cycle_row['사이클 실행여부'], cycle_row['생산기준대수'], cycle_row['우선순위'], cycle_row['후순위']]
                    list_isModelEqual = [merge_order_row['대표모델'][k] == cycle_row[f'{str(k+1)}'] or str(cycle_row[f'{str(k+1)}']) == 'nan' for k in range(len(merge_order_row['대표모델']))]
                    list_isModelEqual.append(str(cycle_row['OptionCode']) in str(merge_order_row['MS Code']) or str(cycle_row['OptionCode']) == 'nan')

                    if self.trueCheck(list_isModelEqual):
                        df_mergeOrderResult.at[i, '사이클 그룹'] = cycle_row['No']
                        df_mergeOrderResult.at[i, '우선순위'] = cycle_row['우선순위']
                        df_mergeOrderResult.at[i, '후순위'] = cycle_row['후순위']
                        break

            df_mergeOrderResult = df_mergeOrderResult.sort_values(by=['우선순위', '후순위'], ascending=[True, False])
            df_mergeOrderResult = df_mergeOrderResult.reset_index(drop=True)
            dict_cycleGr = {}
            for group, data in df_mergeOrderResult.groupby('사이클 그룹'):
                dict_cycleGr[group] = data

            df_firstOrder = pd.DataFrame().reindex_like(df_mergeOrderResult)
            df_firstOrder = df_firstOrder[0:0]
            df_secondOrder = df_firstOrder.copy()

            firstModCnt = int(self.firstModCnt)
            secondModCnt = int(self.secondModCnt)
            totalRowCount = firstModCnt + secondModCnt

            for group, data in dict_cycleGr.items():
                groupLen = len(data)
                secondGrLen = int(groupLen * (secondModCnt / (firstModCnt + secondModCnt)))

                suffledData = data.sample(frac=1, random_state=42)

                df_firstOrderTemp = suffledData[secondGrLen:]
                df_secondOrderTemp = suffledData[:secondGrLen]

                df_firstOrder = pd.concat([df_firstOrder, df_firstOrderTemp])
                df_secondOrder = pd.concat([df_secondOrder, df_secondOrderTemp])

                if len(df_firstOrder) >= firstModCnt and len(df_secondOrder) >= secondModCnt:
                    break

            df_firstOrder = df_firstOrder.sample(n=firstModCnt, random_state=42)
            df_secondOrder = df_secondOrder.sample(n=secondModCnt, replace=True, random_state=42)

            df_firstOrder = df_firstOrder.reset_index(drop=True)
            df_secondOrder = df_secondOrder.reset_index(drop=True)

            # 두 dataframe의 행 수가 self.firstModCnt와 self.secondModCnt보다 작을 경우, 남은 행을 임의로 채워줌
            remainingRowCount = totalRowCount - len(df_firstOrder) - len(df_secondOrder)
            if remainingRowCount > 0:
                remainingData = df_mergeOrderResult[~df_mergeOrderResult.index.isin(df_firstOrder.index) & ~df_mergeOrderResult.index.isin(df_secondOrder.index)]
                remainingData = remainingData.sample(n=remainingRowCount, random_state=42)
                df_firstOrder = pd.concat([df_firstOrder, remainingData.iloc[:remainingRowCount // 2]])
                df_secondOrder = pd.concat([df_secondOrder, remainingData.iloc[remainingRowCount // 2:]])

            df_firstOrder = df_firstOrder.reset_index(drop=True)
            df_secondOrder = df_secondOrder.reset_index(drop=True)

            if self.isDebug:
                df_firstOrder.to_excel('.\\debug\\JUXTA\\Old\\1st.xlsx')
                df_secondOrder.to_excel('.\\debug\\JUXTA\\Old\\2nd.xlsx')

            list_df = [df_firstOrder, df_secondOrder]

            for df_split in list_df:
                df_priority = df_split[df_split['우선순위'] != '-']
                df_unPriority = df_split[df_split['후순위'] != '-']
                df_priority = df_priority.reset_index(drop=True)
                df_unPriority = df_unPriority.reset_index(drop=True)
                if self.isDebug:
                    df_split.to_excel('.\\debug\\JUXTA\\Old\\flow9.xlsx')

                df_cycleResult = pd.DataFrame().reindex_like(df_priority)
                df_cycleResult = df_cycleResult[0:0]
                prioritySize = len(df_priority)
                while len(df_cycleResult) < prioritySize:
                    for key, value in dict_cycleNo.items():
                        cycleCnt = 0
                        list_deleteRow = []
                        list_deleteKey = []
                        for i in df_priority.index:
                            if key == str(df_priority['사이클 그룹'][i]):
                                if ((type(value[1]) is int or type(value[1]) is float) and cycleCnt <= value[1]) or value[1] == '-':
                                    df_cycleResult = df_cycleResult.append(df_priority.iloc[i])
                                    list_deleteRow.append(i)
                                    cycleCnt += 1
                            if cycleCnt == value[1]:
                                break
                        if value[0] not in ['〇', 'O']:
                            list_deleteKey.append(key)
                        df_priority = df_priority.drop(list_deleteRow)
                        df_priority = df_priority.reset_index(drop=True)

                    for deleteKey in list_deleteKey:
                        if deleteKey in dict_cycleNo.keys():
                            del dict_cycleNo[deleteKey]

                df_unPriority = df_unPriority.sort_values(by=['후순위'], ascending=[False])
                df_cycleResult = pd.concat([df_cycleResult, df_unPriority])
                df_cycleResult = df_cycleResult.reset_index(drop=True)

                if self.isDebug:
                    df_cycleResult.to_excel('.\\debug\\JUXTA\\Old\\flow10.xlsx')

                df_cycleResult['No (*)'] = (df_cycleResult.index.astype(int) + 1) * 10
                df_cycleResult['Scheduled Start Date (*)'] = self.constDate
                df_cycleResult['Planned Order'] = df_cycleResult['Planned Order'].astype(int).astype(str).str.zfill(10)
                df_cycleResult['Scheduled End Date'] = df_cycleResult['Scheduled End Date'].astype(str).str.zfill(10)
                df_cycleResult['Specified Start Date'] = df_cycleResult['Specified Start Date'].astype(str).str.zfill(10)
                df_cycleResult['Specified End Date'] = df_cycleResult['Specified End Date'].astype(str).str.zfill(10)
                df_cycleResult['Spec Freeze Date'] = df_cycleResult['Spec Freeze Date'].astype(str).str.zfill(10)
                df_cycleResult['Component Number'] = df_cycleResult['Component Number'].astype(int).astype(str).str.zfill(4)
                df_cycleResult = df_cycleResult[['No (*)',
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
                    if len(df_cycleResult[df_cycleResult['Linkage Number'] == df_emgLinkage['Linkage Number'][i]]['Linkage Number'].values) > 0:
                        dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = True
                    else:
                        dict_emgLinkage[df_emgLinkage['Linkage Number'][i]] = False
                for i in df_emgmscode.index:
                    if len(df_cycleResult[df_cycleResult['MS Code'] == df_emgmscode['MS-CODE'][i]]['MS Code'].values) > 0:
                        dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = True
                    else:
                        dict_emgMscode[df_emgLinkage['MS-CODE'][i]] = False

                # self.spReturnEmgLinkage.emit(dict_emgLinkage)
                # self.spReturnEmgMscode.emit(dict_emgMscode)
                if df_split.equals(df_firstOrder):
                    path = f'.\\Result\\JUXTA\\{str(today)}\\1차'
                else:
                    path = f'.\\Result\\JUXTA\\{str(today)}\\2차'
                if not os.path.exists(path):
                    os.makedirs(path)
                outputFile = f'{path}\\{str(today)}_Old.xlsx'
                df_cycleResult.to_excel(outputFile, index=False)

    def trueCheck(self, list):
        for item in list:
            if not item:
                return False
        return True

