from datetime import datetime
import os
import glob
import pandas as pd
import logging
import numpy as np
import math
import re
from logging.handlers import RotatingFileHandler
from PyQt5.QtCore import Qt, QCoreApplication
from PyQt5.QtGui import QStandardItemModel, QIcon, QIntValidator
from PyQt5.QtWidgets import QMainWindow, QProgressBar, QWidget, QGridLayout, QGroupBox, QLineEdit, QSizePolicy, QToolButton, QLabel, QFrame, QListView, QMenuBar, QStatusBar, QPushButton, QCheckBox
from PyQt5.QtCore import pyqtSlot, QThread, QRect, QSize
from Worker import Worker
from CustomFormatter import CustomFormatter
from UI_SubWindow import UI_SubWindow
from CalendarWindow import CalendarWindow
from QPlainTextEditLogger import QPlainTextEditLogger


class Ui_UtaMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi()

    def setupUi(self):
        rfh = RotatingFileHandler(filename='./Log.log',
                                    mode='a',
                                    maxBytes=5 * 1024 * 1024,
                                    backupCount=2,
                                    encoding=None,
                                    delay=0)
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s:%(levelname)s:%(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S',
                            handlers=[rfh])
        self.setObjectName('MainWindow')
        self.resize(800, 800)
        self.setStyleSheet('background-color: rgb(252, 252, 252);')
        self.centralwidget = QWidget(self)
        self.centralwidget.setObjectName('centralwidget')
        self.gridLayout2 = QGridLayout(self.centralwidget)
        self.gridLayout2.setObjectName('gridLayout2')
        self.gridLayout = QGridLayout()
        self.gridLayout.setObjectName('gridLayout')
        self.groupBox = QGroupBox(self.centralwidget)
        self.groupBox.setTitle('')
        self.groupBox.setObjectName('groupBox')
        self.gridLayout4 = QGridLayout(self.groupBox)
        self.gridLayout4.setObjectName('gridLayout4')
        self.gridLayout3 = QGridLayout()
        self.gridLayout3.setObjectName('gridLayout3')
        self.maxOrderinput = QLineEdit(self.groupBox)
        self.maxOrderinput.setMinimumSize(QSize(0, 25))
        self.maxOrderinput.setObjectName('maxOrderinput')
        self.maxOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.maxOrderinput, 0, 1, 1, 3)
        self.dateBtn = QToolButton(self.groupBox)
        self.dateBtn.setMinimumSize(QSize(0, 25))
        self.dateBtn.setObjectName('dateBtn')
        self.gridLayout3.addWidget(self.dateBtn, 1, 1, 1, 1)
        self.emgFileInputBtn = QPushButton(self.groupBox)
        self.emgFileInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.emgFileInputBtn, 2, 1, 1, 1)
        self.holdFileInputBtn = QPushButton(self.groupBox)
        self.holdFileInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.holdFileInputBtn, 5, 1, 1, 1)
        self.label4 = QLabel(self.groupBox)
        self.label4.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label4.setObjectName('label4')
        self.gridLayout3.addWidget(self.label4, 3, 1, 1, 1)
        self.label5 = QLabel(self.groupBox)
        self.label5.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label5.setObjectName('label5')
        self.gridLayout3.addWidget(self.label5, 3, 2, 1, 1)
        self.label6 = QLabel(self.groupBox)
        self.label6.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label6.setObjectName('label6')
        self.gridLayout3.addWidget(self.label6, 6, 1, 1, 1)
        self.label7 = QLabel(self.groupBox)
        self.label7.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label7.setObjectName('label7')
        self.gridLayout3.addWidget(self.label7, 6, 2, 1, 1)
        listViewModelEmgLinkage = QStandardItemModel()
        self.listViewEmgLinkage = QListView(self.groupBox)
        self.listViewEmgLinkage.setModel(listViewModelEmgLinkage)
        self.gridLayout3.addWidget(self.listViewEmgLinkage, 4, 1, 1, 1)
        listViewModelEmgmscode = QStandardItemModel()
        self.listViewEmgmscode = QListView(self.groupBox)
        self.listViewEmgmscode.setModel(listViewModelEmgmscode)
        self.gridLayout3.addWidget(self.listViewEmgmscode, 4, 2, 1, 1)
        listViewModelHoldLinkage = QStandardItemModel()
        self.listViewHoldLinkage = QListView(self.groupBox)
        self.listViewHoldLinkage.setModel(listViewModelHoldLinkage)
        self.gridLayout3.addWidget(self.listViewHoldLinkage, 7, 1, 1, 1)
        listViewModelHoldmscode = QStandardItemModel()
        self.listViewHoldmscode = QListView(self.groupBox)
        self.listViewHoldmscode.setModel(listViewModelHoldmscode)
        self.gridLayout3.addWidget(self.listViewHoldmscode, 7, 2, 1, 1)
        self.labelBlank = QLabel(self.groupBox)
        self.labelBlank.setObjectName('labelBlank')
        self.gridLayout3.addWidget(self.labelBlank, 2, 4, 1, 1)
        self.progressbar = QProgressBar(self.groupBox)
        self.progressbar.setObjectName('progressbar')
        self.gridLayout3.addWidget(self.progressbar, 10, 1, 1, 2)
        thread = QThread()
        worker = Worker()
        worker.moveToThread(thread)
        worker.progressChanged.connect(self.progressbar.setValue)
        self.runBtn = QToolButton(self.groupBox)
        sizePolicy = QSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.runBtn.sizePolicy().hasHeightForWidth())
        self.runBtn.setSizePolicy(sizePolicy)
        self.runBtn.setMinimumSize(QSize(0, 35))
        self.runBtn.setStyleSheet('background-color: rgb(63, 63, 63);\ncolor: rgb(255, 255, 255);')
        self.runBtn.setObjectName('runBtn')
        self.gridLayout3.addWidget(self.runBtn, 10, 3, 1, 2)
        self.label = QLabel(self.groupBox)
        self.label.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label.setObjectName('label')
        self.gridLayout3.addWidget(self.label, 0, 0, 1, 1)
        self.label8 = QLabel(self.groupBox)
        self.label8.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label8.setObjectName('label8')
        self.gridLayout3.addWidget(self.label8, 1, 0, 1, 1)
        self.labelDate = QLabel(self.groupBox)
        self.labelDate.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.labelDate.setObjectName('labelDate')
        self.gridLayout3.addWidget(self.labelDate, 1, 2, 1, 1)
        self.label2 = QLabel(self.groupBox)
        self.label2.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label2.setObjectName('label2')
        self.gridLayout3.addWidget(self.label2, 2, 0, 1, 1)
        self.label3 = QLabel(self.groupBox)
        self.label3.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label3.setObjectName('label3')
        self.gridLayout3.addWidget(self.label3, 5, 0, 1, 1)
        self.cbLimit = QCheckBox('UT32A, UT52A, UM33A 착공량 50%제한', self)
        self.cbLimit.setObjectName('cbLimit')
        self.gridLayout3.addWidget(self.cbLimit, 8, 1, 1, 2)
        self.cbLimit.setChecked(True)
        self.line = QFrame(self.groupBox)
        self.line.setFrameShape(QFrame.HLine)
        self.line.setFrameShadow(QFrame.Sunken)
        self.line.setObjectName('line')
        self.gridLayout3.addWidget(self.line, 9, 0, 1, 10)
        self.gridLayout4.addLayout(self.gridLayout3, 0, 0, 1, 1)
        self.gridLayout.addWidget(self.groupBox, 0, 0, 1, 1)
        self.groupBox2 = QGroupBox(self.centralwidget)
        self.groupBox2.setTitle('')
        self.groupBox2.setObjectName('groupBox2')
        self.gridLayout6 = QGridLayout(self.groupBox2)
        self.gridLayout6.setObjectName('gridLayout6')
        self.gridLayout5 = QGridLayout()
        self.gridLayout5.setObjectName('gridLayout5')
        self.logBrowser = QPlainTextEditLogger(self.groupBox2)
        self.logBrowser.widget.setParent(self)
        self.logBrowser.setFormatter(CustomFormatter())
        logging.getLogger().addHandler(self.logBrowser)
        logging.getLogger().setLevel(logging.INFO)
        self.gridLayout5.addWidget(self.logBrowser.widget, 0, 0, 1, 1)
        self.gridLayout6.addLayout(self.gridLayout5, 0, 0, 1, 1)
        self.gridLayout.addWidget(self.groupBox2, 1, 0, 1, 1)
        self.gridLayout2.addLayout(self.gridLayout, 0, 0, 1, 1)
        self.setCentralWidget(self.centralwidget)
        self.menubar = QMenuBar(self)
        self.menubar.setGeometry(QRect(0, 0, 653, 21))
        self.menubar.setObjectName('menubar')
        self.setMenuBar(self.menubar)
        self.statusbar = QStatusBar(self)
        self.statusbar.setObjectName('statusbar')
        self.setStatusBar(self.statusbar)
        self.retranslateUi(self)
        self.dateBtn.clicked.connect(self.selectStartDate)
        self.emgFileInputBtn.clicked.connect(self.emgWindow)
        self.holdFileInputBtn.clicked.connect(self.holdWindow)
        self.runBtn.clicked.connect(self.startLeveling)
        self.cbLimit.stateChanged.connect(self.changeCbLimit)
        # 디버그용 플래그
        self.isDebug = True
        if self.isDebug:
            self.debugDate = QLineEdit(self.groupBox)
            self.debugDate.setObjectName('debugDate')
            self.gridLayout3.addWidget(self.debugDate, 10, 0, 1, 1)
            self.debugDate.setPlaceholderText('디버그용 날짜입력')
        self.show()

    def retranslateUi(self, MainWindow):
        _translate = QCoreApplication.translate
        MainWindow.setWindowTitle(_translate('MainWindow', 'UTA 착공량 평준화 프로그램 Rev1.03'))
        MainWindow.setWindowIcon(QIcon('.\\Logo\\logo.png'))
        self.label.setText(_translate('MainWindow', '생산대수 (최대 착공량):'))
        self.runBtn.setText(_translate('MainWindow', '실행'))
        self.label2.setText(_translate('MainWindow', '긴급오더 입력 :'))
        self.label3.setText(_translate('MainWindow', '홀딩오더 입력 :'))
        self.label4.setText(_translate('MainWindow', 'Linkage No List'))
        self.label5.setText(_translate('MainWindow', 'MSCode List'))
        self.label6.setText(_translate('MainWindow', 'Linkage No List'))
        self.label7.setText(_translate('MainWindow', 'MSCode List'))
        self.label8.setText(_translate('MainWndow', '착공지정일 입력 :'))
        self.labelDate.setText(_translate('MainWndow', '미선택'))
        self.dateBtn.setText(_translate('MainWindow', ' 착공지정일 선택 '))
        self.emgFileInputBtn.setText(_translate('MainWindow', '리스트 입력'))
        self.holdFileInputBtn.setText(_translate('MainWindow', '리스트 입력'))
        self.labelBlank.setText(_translate('MainWindow', '            '))
        logging.info('프로그램이 정상 기동했습니다')

    @pyqtSlot()
    def emgWindow(self):
        list_emgHold = self.loadEmgHoldList()
        self.sub_window = UI_SubWindow(list_emgHold[0], list_emgHold[1])
        self.sub_window.submitClicked.connect(self.getEmgListview)

    @pyqtSlot()
    def holdWindow(self):
        list_emgHold = self.loadEmgHoldList()
        self.sub_window = UI_SubWindow(list_emgHold[2], list_emgHold[3])
        self.sub_window.submitClicked.connect(self.getHoldListview)

    def getEmgListview(self, list):
        if len(list) > 0:
            self.listViewEmgLinkage.setModel(list[0])
            self.listViewEmgmscode.setModel(list[1])
            logging.info('긴급오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('긴급오더 리스트가 없습니다. 다시 한번 확인해주세요')

    def getHoldListview(self, list):
        if len(list) > 0:
            self.listViewHoldLinkage.setModel(list[0])
            self.listViewHoldmscode.setModel(list[1])
            logging.info('홀딩오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('긴급오더 리스트가 없습니다. 다시 한번 확인해주세요')

    def updateProgressbar(self, val):
        self.progressbar.setValue(val)

    def selectStartDate(self):
        self.calendar_window = CalendarWindow()
        self.calendar_window.submitClicked.connect(self.getDate)

    def getDate(self, date):
        if len(date) > 0:
            self.labelDate.setText(date)
            logging.info('착공지정일이 %s 로 정상적으로 지정되었습니다.', date)
        else:
            logging.error('착공지정일이 선택되지 않았습니다.')

    @pyqtSlot()
    def changeCbLimit(self):
        if self.cbLimit.isChecked():
            logging.info('UT32A, UT52A, UM33A 착공량이 전체 착공량의 50%로 제한됩니다.')
        else:
            logging.info('UT32A, UT52A, UM33A 착공량 제한이 해제되었습니다.')

    # 긴급/홀딩오더 리스트 불러오기
    def loadEmgHoldList(self):
        list_emgHold = []
        list_emgHold.append([str(self.listViewEmgLinkage.model().data(self.listViewEmgLinkage.model().index(x, 0))) for x in range(self.listViewEmgLinkage.model().rowCount())])
        list_emgHold.append([self.listViewEmgmscode.model().data(self.listViewEmgmscode.model().index(x, 0)) for x in range(self.listViewEmgmscode.model().rowCount())])
        list_emgHold.append([str(self.listViewHoldLinkage.model().data(self.listViewHoldLinkage.model().index(x, 0))) for x in range(self.listViewHoldLinkage.model().rowCount())])
        list_emgHold.append([self.listViewHoldmscode.model().data(self.listViewHoldmscode.model().index(x, 0)) for x in range(self.listViewHoldmscode.model().rowCount())])
        return list_emgHold

    @pyqtSlot()
    def startLeveling(self):
        # 마스터파일 불러오기용 내부함수
        def loadMasterFile():
            masterFileList = []
            date = datetime.today().strftime('%Y%m%d')
            if self.isDebug:
                date = self.debugDate.text()
            utaOrderFilePath = r'.\\input\\UTA\\Master_File\\' + date + r'\\UTA 착공 ' + date[4:] + r' (착공 수주 DATA).xlsx'
            levelingListFilePath = r'.\\input\\UTA\\Master_File\\' + date + r'\\5400_A0100A81_' + date + r'_Leveling_List.xlsx'
            conditionFilePath = r'.\\input\\UTA\\mscODE_Table\\UTA_기종분류_기준표.xlsx'
            calendarFilePath = r'.\\input\\UTA\\Calendar_File\\FY' + date[2:4] + '_Calendar.xlsx'
            if os.path.exists(utaOrderFilePath):
                if os.path.exists(levelingListFilePath):
                    if os.path.exists(conditionFilePath):
                        if os.path.exists(calendarFilePath):
                            utaOrderFile = glob.glob(utaOrderFilePath)[0]
                            levelingListFile = glob.glob(levelingListFilePath)[0]
                            conditionFile = glob.glob(conditionFilePath)[0]
                            caledarFile = glob.glob(calendarFilePath)[0]
                            masterFileList.append(utaOrderFile)
                            masterFileList.append(levelingListFile)
                            masterFileList.append(conditionFile)
                            masterFileList.append(caledarFile)
                            logging.info('마스터 파일 및 기종분류 기준표를 정상적으로 불러왔습니다.')
                        else:
                            logging.error('%s 파일이 없습니다. 확인해주세요.', calendarFilePath)
                            self.runBtn.setEnabled(True)
                    else:
                        logging.error('%s 파일이 없습니다. 확인해주세요.', conditionFilePath)
                        self.runBtn.setEnabled(True)
                else:
                    logging.error('%s 파일이 없습니다. 확인해주세요.', levelingListFilePath)
                    self.runBtn.setEnabled(True)
            else:
                logging.error('%s 파일이 없습니다. 확인해주세요.', utaOrderFilePath)
                self.runBtn.setEnabled(True)
            return masterFileList

        def checkWorkDay(df, today, compDate):
            dtToday = pd.to_datetime(datetime.strptime(today, '%Y%m%d'))
            dtComp = pd.to_datetime(compDate, unit='s')
            workDay = 0
            for i in df.index:
                dt = pd.to_datetime(df['Date'][i], unit='s')
                if dtToday < dt and dt <= dtComp:
                    if df['WorkingDay'][i] == 1:
                        workDay += 1
            return workDay

        self.runBtn.setEnabled(False)

        # 50%제한 플래그
        limitFlag = False
        if self.cbLimit.isChecked():
            limitFlag = True

        # pandas 경고 출력 OFF
        pd.set_option('mode.chained_assignment', None)
        try:
            listMasterFile = loadMasterFile()
            # 총 착공량 입력 확인 후, 메인로직 실행
            if len(self.maxOrderinput.text()) > 0 and len(listMasterFile) > 0 and int(self.maxOrderinput.text()) != 0:
                maxConstructCnt = float(self.maxOrderinput.text())
                # 리스트뷰의 긴급오더, 홀딩오더 대상을 리스트화
                emgLinkage = [str(self.listViewEmgLinkage.model().data(self.listViewEmgLinkage.model().index(x, 0))) for x in range(self.listViewEmgLinkage.model().rowCount())]
                emgmscode = [self.listViewEmgmscode.model().data(self.listViewEmgmscode.model().index(x, 0)) for x in range(self.listViewEmgmscode.model().rowCount())]
                holdLinkage = [str(self.listViewHoldLinkage.model().data(self.listViewHoldLinkage.model().index(x, 0))) for x in range(self.listViewHoldLinkage.model().rowCount())]
                holdmscode = [self.listViewHoldmscode.model().data(self.listViewHoldmscode.model().index(x, 0)) for x in range(self.listViewHoldmscode.model().rowCount())]
                # 긴급오더, 홀딩오더 리스트를 DataFrame으로 변환
                dfEmgLinkage = pd.DataFrame({'Linkage Number': emgLinkage})
                dfEmgmscode = pd.DataFrame({'MS Code': emgmscode})
                dfHoldLinkage = pd.DataFrame({'Linkage Number': holdLinkage})
                dfHoldmscode = pd.DataFrame({'MS Code': holdmscode})
                # 각 Linkage Number Column을 int64 Type으로 변환
                dfEmgLinkage['Linkage Number'] = dfEmgLinkage['Linkage Number'].astype(np.int64)
                dfHoldLinkage['Linkage Number'] = dfHoldLinkage['Linkage Number'].astype(np.int64)
                # Merge 전, 컬럼에 긴급, 홀딩 오더 대상을 표기
                dfEmgLinkage['긴급오더'] = '대상'
                dfEmgmscode['긴급오더'] = '대상'
                dfHoldLinkage['홀딩오더'] = '대상'
                dfHoldmscode['홀딩오더'] = '대상'
                # Leveling Data 불러오기
                dfLeveling = pd.read_excel(listMasterFile[1], converters={'Component Number': str})
                if self.isDebug:
                    dfLeveling.to_excel('.\\debug\\UTA\\flow1.xlsx')
                # 미착공 대상 식별 후, 그 외는 삭제
                dfLevelingDropSEQ = dfLeveling[dfLeveling['Sequence No'].isnull()]
                dfLevelingUndepSeq = dfLeveling[dfLeveling['Sequence No'] == 'Undep']
                dfLevelingUncorSeq = dfLeveling[dfLeveling['Sequence No'] == 'Uncor']
                dfLevelingResult = pd.concat([dfLevelingDropSEQ, dfLevelingUndepSeq, dfLevelingUncorSeq])
                dfLevelingResult = dfLevelingResult.reset_index(level=None, drop=False, inplace=False)
                for i in dfLevelingResult.index:
                    if len(str(dfLevelingResult['Component Number'][i])) != 4:
                        logging.warning(f'「Planned Order : {str(dfLevelingResult["Planned Order"][i]).zfill(10)}」의 Component Number를 확인해주세요. (자릿수 이상)')

                dfLevelingResult['미착공수량'] = dfLevelingResult.groupby('Linkage Number')['Linkage Number'].transform('size')
                dfUtaOrder = pd.read_excel(listMasterFile[0])
                if self.isDebug:
                    dfUtaOrder.to_excel('.\\debug\\UTA\\flow2.xlsx')
                dfMerge = pd.merge(dfUtaOrder, dfLevelingResult, on='Linkage Number').drop_duplicates(['Linkage Number'])
                if self.isDebug:
                    dfMerge.to_excel('.\\debug\\UTA\\flow3.xlsx')
                dfMergeLink = pd.merge(dfMerge, dfEmgLinkage, on='Linkage Number', how='left')
                dfMergemscode = pd.merge(dfMerge, dfEmgmscode, on='MS Code', how='left')
                dfMergeLink = pd.merge(dfMergeLink, dfHoldLinkage, on='Linkage Number', how='left')
                if self.isDebug:
                    dfMergeLink.to_excel('.\\debug\\UTA\\flow4.xlsx')
                dfMergemscode = pd.merge(dfMergemscode, dfHoldmscode, on='MS Code', how='left')
                # 디버그용 파일출력
                if self.isDebug:
                    dfMergemscode.to_excel('.\\debug\\UTA\\flow5.xlsx')
                dfMergeLink['긴급오더'] = dfMergeLink['긴급오더'].combine_first(dfMergemscode['긴급오더'])
                dfMergeLink['홀딩오더'] = dfMergeLink['홀딩오더'].combine_first(dfMergemscode['홀딩오더'])
                dfMergeLink['Linkage Number'] = dfMergeLink['Linkage Number'].astype(str)
                # 디버그용 파일출력
                if self.isDebug:
                    dfMergeLink.to_excel('.\\debug\\UTA\\flow6.xlsx')
                # 필요한 Column만 추출
                dfMergeResult = dfMergeLink[['Country: Ship-to Party',
                                                'Linkage Number',
                                                'Material',
                                                'MS Code',
                                                'Status Category',
                                                'Order Quantity',
                                                '미착공수량',
                                                'Planned Prod. Completion date',
                                                'Planned Shipping date',
                                                '긴급오더',
                                                '홀딩오더']]
                # 결과물을 Copy할 빈 DataFrame 생성
                dfCopy = pd.DataFrame(columns=['Country: Ship-to Party',
                                                'Linkage Number',
                                                'Material',
                                                'MS Code',
                                                'Status Category',
                                                'Order Quantity',
                                                '미착공수량',
                                                'Planned Prod. Completion date',
                                                'Planned Shipping date',
                                                '특수사양',
                                                '일반사양',
                                                '긴급오더',
                                                '홀딩오더'])
                # 디버그용 파일출력
                if self.isDebug:
                    dfMergeResult.to_excel('.\\debug\\UTA\\flow7.xlsx')
                # 완성지정일과 출하지정일이 급한 순으로 나열
                dfMergeResultS = dfMergeResult.sort_values(by=['Planned Prod. Completion date', 'Planned Shipping date'], ascending=[True, True])
                # 정렬에 따른 인덱스 재설정
                dfMergeResultS = dfMergeResultS.reset_index(level=None, drop=False, inplace=False)
                # 추후 추가할 데이터 Column생성
                dfMergeResultS['공수배율'] = 0
                dfMergeResultS['특수사양'] = ""
                dfMergeResultS['일반사양'] = ""
                dfMergeResultS['UT32A, UT52A, UM33A구분여부'] = ""
                dfMergeResultS['Cycle기준'] = np.nan
                dfMergeResultS['MAX 착공 필요'] = ""
                dfMergeResultS['착공비율(%)'] = ""
                dfMergeResultS['남은 워킹데이'] = np.nan
                # 기종 구분 기준표 불러오기
                dfCondition = pd.read_excel(listMasterFile[2])
                # 셀 병합시 바로 이전 값을 가지고옴
                dfCondition['No'] = dfCondition['No'].fillna(method='ffill')
                dfCondition['日(LINE)가능대수'] = dfCondition['日(LINE)가능대수'].fillna(method='ffill')
                dfCondition['착공비율(%)'] = dfCondition['착공비율(%)'].fillna(method='ffill')
                dfCondition['착공비율(대수)'] = dfCondition['착공비율(대수)'].fillna(method='ffill')
                dfCondition['Cycle 기준 대수'] = dfCondition['Cycle 기준 대수'].fillna(method='ffill')
                dfCondition['공수 배율'] = dfCondition['공수 배율'].fillna(method='ffill')
                dfCondition['특수 구분 (우선 순위)'] = dfCondition['특수 구분 (우선 순위)'].fillna(method='ffill')
                dfCondition['MAX 착공 필요'] = dfCondition['MAX 착공 필요'].fillna(method='ffill')
                # 최대착공가능량 및 사양별 착공량, UT용 착공량 저장을 위한 변수 선언
                constructTempCnt = float(maxConstructCnt)
                capableCntDic = {}
                preOrderDic = {}
                # 기종 분류 기준표와 착공 수주 Data의 비교로직
                for i in dfCondition.index:
                    # 일 가능대수 없을 경우, 최대착공량과 동일하게 변경
                    if dfCondition['日(LINE)가능대수'][i] == '-':
                        capableCntDic[float(dfCondition['No'][i])] = maxConstructCnt
                    else:
                        capableCntDic[float(dfCondition['No'][i])] = dfCondition['日(LINE)가능대수'][i]
                        preOrderDic[float(dfCondition['No'][i])] = math.ceil(float(dfCondition['日(LINE)가능대수'][i]) * float(dfCondition['착공비율(%)'][i]))
                for i in dfMergeResultS.index:
                    # 불요 모델 4850U1-□, UTAP□□□ 삭제
                    if str(dfMergeResultS['MS Code'][i]).find('4850U1') > -1 or str(dfMergeResultS['MS Code'][i]).find('UTAP') > -1:
                        dfMergeResultS.drop(i, inplace=True)
                    # Status 100이상이나 0인 경우 삭제
                    if int(dfMergeResultS['Status Category'][i]) >= 100 or int(dfMergeResultS['Status Category'][i]) == 0:
                        dfMergeResultS.drop(i, inplace=True)
                    # UT용 구분
                    if str(dfMergeResultS['MS Code'][i]).find('UT32A') > -1 or str(dfMergeResultS['MS Code'][i]).find('UT52A') > -1 or str(dfMergeResultS['MS Code'][i]).find('UM33A') > -1:
                        dfMergeResultS['UT32A, UT52A, UM33A구분여부'][i] = 'UT32A, UT52A, UM33A'
                    # 착공 지정일이 당일보다 앞일 경우, 긴급오더로 지정
                    dt = pd.to_datetime(dfMergeResultS['Planned Prod. Completion date'][i], unit='s')
                    if self.labelDate.text() != '미선택':
                        dtInput = pd.to_datetime(datetime.strptime(self.labelDate.text(), '%Y-%m-%d'))
                    if self.labelDate.text() == '미선택':
                        if dt < pd.Timestamp.now():
                            dfMergeResultS['긴급오더'][i] = '대상'
                    else:
                        if dt < dtInput:
                            dfMergeResultS['긴급오더'][i] = '대상'
                    # 기종분류 기준표 불러오기
                    for j in dfCondition.index:
                        if i == 58 or i == 275 or i == 670:
                            print()
                        optionCode = ""
                        mscode = ''
                        if str(dfCondition['Model'][j]).strip() != 'nan' and str(dfCondition['Model'][j]).strip() != '':
                            mscode = str(dfCondition['Model'][j]).strip()
                        else:
                            splitList = str(dfMergeResultS['MS Code'][i]).split('-')
                            for k in range(0, len(splitList[0])):
                                mscode += '.'
                        if 'LL50A' not in dfMergeResultS['MS Code'][i] and 'MDUT' not in dfMergeResultS['MS Code'][i]:
                            for k in range(1, 8):
                                if str(dfCondition['Suffix Code' + str(k)][j]).strip() != 'nan' and str(dfCondition['Suffix Code' + str(k)][j]).strip() != '':
                                    if 'UM33A' not in str(dfMergeResultS['MS Code'][i]).strip():
                                        if k in (1, 4, 6):
                                            mscode = mscode + '-' + str(dfCondition['Suffix Code' + str(k)][j]).strip()
                                        else:
                                            mscode = mscode + str(dfCondition['Suffix Code' + str(k)][j]).strip()
                                    else:
                                        if k in (1, 4):
                                            mscode = mscode + '-' + str(dfCondition['Suffix Code' + str(k)][j]).strip()
                                        elif k < 6:
                                            mscode = mscode + str(dfCondition['Suffix Code' + str(k)][j]).strip()
                                else:
                                    if 'UM33A' not in str(dfMergeResultS['MS Code'][i]).strip():
                                        if k in (1, 4, 6):
                                            mscode = mscode + '-' + '.'
                                        else:
                                            mscode = mscode + '.'
                                    else:
                                        if k in (1, 4):
                                            mscode = mscode + '-' + '.'
                                        elif k < 6:
                                            mscode = mscode + '.'
                        if str(dfCondition['Option Code'][j]).strip() != 'nan' and str(dfCondition['Option Code'][j]).strip() != '':
                            optionCode = str(dfCondition['Option Code'][j]).strip()
                        else:
                            optionCode = ""
                        # if optionCode == '/DC' and str(dfMergeResultS['MS Code'][i]) == 'UM33A-020-00/DC':
                        #     print()
                        if bool(re.search(mscode, str(dfMergeResultS['MS Code'][i]))):
                            if optionCode != "":
                                if str(dfMergeResultS['MS Code'][i]).find(optionCode) > -1:
                                    if dfCondition['특수 구분 (우선 순위)'][j] == '특수':
                                        if dfMergeResultS['특수사양'][i] == "":
                                            dfMergeResultS['특수사양'][i] = str(dfCondition['No'][j])
                                        else:
                                            dfMergeResultS['특수사양'][i] = dfMergeResultS['특수사양'][i] + ',' + str(dfCondition['No'][j])
                                    else:
                                        if dfMergeResultS['일반사양'][i] == "":
                                            dfMergeResultS['일반사양'][i] = str(dfCondition['No'][j])
                                        else:
                                            dfMergeResultS['일반사양'][i] = dfMergeResultS['일반사양'][i] + ',' + str(dfCondition['No'][j])
                                    if dfCondition['Cycle 기준 대수'][j] != '-':
                                        dfMergeResultS['Cycle기준'][i] = int(dfCondition['Cycle 기준 대수'][j])
                                    if dfMergeResultS['공수배율'][i] == "" or dfMergeResultS['공수배율'][i] < dfCondition['공수 배율'][j]:
                                        dfMergeResultS['공수배율'][i] = dfCondition['공수 배율'][j]
                                    dfMergeResultS['착공비율(%)'][i] = dfCondition['착공비율(%)'][j]
                                    if str(dfCondition['MAX 착공 필요'][j]) != 'nan' and str(dfCondition['MAX 착공 필요'][j]) != '' and str(dfCondition['MAX 착공 필요'][j]) != '-':
                                        dfMergeResultS['MAX 착공 필요'][i] = '대상'
                                        dfMergeResultS['착공비율(%)'][i] = 1.0
                            elif mscode != '.....-...-..' and mscode != '.....' and mscode != '....':
                                if dfCondition['특수 구분 (우선 순위)'][j] == '특수':
                                    if dfMergeResultS['특수사양'][i] == "":
                                        dfMergeResultS['특수사양'][i] = str(dfCondition['No'][j])
                                    else:
                                        dfMergeResultS['특수사양'][i] = dfMergeResultS['특수사양'][i] + ',' + str(dfCondition['No'][j])
                                else:
                                    if dfMergeResultS['일반사양'][i] == "":
                                        dfMergeResultS['일반사양'][i] = str(dfCondition['No'][j])
                                    else:
                                        dfMergeResultS['일반사양'][i] = dfMergeResultS['일반사양'][i] + ',' + str(dfCondition['No'][j])
                                if dfCondition['Cycle 기준 대수'][j] != '-':
                                    dfMergeResultS['Cycle기준'][i] = int(dfCondition['Cycle 기준 대수'][j])
                                if dfMergeResultS['공수배율'][i] == "" or dfMergeResultS['공수배율'][i] < dfCondition['공수 배율'][j]:
                                    dfMergeResultS['공수배율'][i] = dfCondition['공수 배율'][j]
                                    dfMergeResultS['착공비율(%)'][i] = dfCondition['착공비율(%)'][j]
                                if str(dfCondition['MAX 착공 필요'][j]) != 'nan' and str(dfCondition['MAX 착공 필요'][j]) != '' and str(dfCondition['MAX 착공 필요'][j]) != '-':
                                    dfMergeResultS['MAX 착공 필요'][i] = '대상'
                                    dfMergeResultS['착공비율(%)'][i] = 1.0
                # 조건에 맞지않는 사양은 공수배율 1로 설정, 필요공수도 배율에 맞게 미착공수량*1로 설정
                dfMergeResultS.loc[dfMergeResultS['공수배율'] == 0, '공수배율'] = 1
                # 디버그용 파일 출력
                if self.isDebug:
                    dfMergeResultS.to_excel('.\\debug\\UTA\\flow8.xlsx')
                # 특수사양과 일반사양의 중복 체크를 위한 카운터 선언
                maxSpCommaCnt = 0
                tempSpCommaCnt = 0
                maxNormalCommaCnt = 0
                tempNormalCommaCnt = 0

                # 워킹데이 캘린더 불러오기
                dfCalendar = pd.read_excel(listMasterFile[3])
                today = datetime.today().strftime('%Y%m%d')
                if self.isDebug:
                    today = self.debugDate.text()

                # 특수사양과 일반사양의 중복 개수를 체크 + 워킹데이 입력
                for i in dfMergeResultS.index:
                    tempSpCommaCnt = str(dfMergeResultS['특수사양'][i]).count(',')
                    if int(tempSpCommaCnt) > int(maxSpCommaCnt):
                        maxSpCommaCnt = tempSpCommaCnt
                    tempNormalCommaCnt = str(dfMergeResultS['일반사양'][i]).count(',')
                    if int(tempNormalCommaCnt) > int(maxNormalCommaCnt):
                        maxNormalCommaCnt = tempNormalCommaCnt
                    dfMergeResultS['남은 워킹데이'][i] = checkWorkDay(dfCalendar, today, dfMergeResultS['Planned Prod. Completion date'][i])

                # 특수사양 Column을 분할
                for i in range(0, maxSpCommaCnt + 1):
                    dfMergeResultS['특수사양' + str(i + 1)] = dfMergeResultS['특수사양'].str.split(',').str[i]
                # 일반사양 Column을 분할
                for i in range(0, maxNormalCommaCnt + 1):
                    dfMergeResultS['일반사양' + str(i + 1)] = dfMergeResultS['일반사양'].str.split(',').str[i]
                dfMergeResultS.fillna(0)

                # 디버그용 파일 출력
                if self.isDebug:
                    dfMergeResultS.to_excel('.\\debug\\UTA\\flow9.xlsx')
                # 완성지정일 -> 출하지정일 -> 특수사양 -> 일반사양 순으로 착공지정우선순위 설정을 위해 정렬
                dfMergeResultSf = dfMergeResultS.sort_values(by=['Planned Prod. Completion date', '긴급오더', 'MAX 착공 필요', 'Planned Shipping date', '특수사양', '일반사양'],
                                                                ascending=[True, False, False, True, False, False])
                # 인덱스 재설정을 위해 이전 인덱스 삭제
                dfMergeResultSf.drop('index', axis=1, inplace=True)
                # 인덱스 재설정
                dfMergeResultSfReset = dfMergeResultSf.reset_index(level=None, drop=False, inplace=False)
                # 이전 인덱스 삭제
                dfMergeResultSfReset.drop('index', axis=1, inplace=True)
                # 디버그용 파일 출력
                if self.isDebug:
                    dfMergeResultSfReset.to_excel('.\\debug\\UTA\\flow10.xlsx')
                dfMergeResultSfReset['착공수량'] = dfMergeResultSfReset['미착공수량']

                dfMergeResultSfReset['선행착공대상'] = ''
                integCntDic = {}
                copyCntDic = capableCntDic.copy()
                minContCntDic = {}
                for i in dfMergeResultSfReset.index:
                    for key, value in capableCntDic.items():
                        for x in range(0, maxSpCommaCnt + 1):
                            if str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != '' and str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != 'nan':
                                if key == float(dfMergeResultSfReset['특수사양' + str(x + 1)][i]):
                                    if key in integCntDic:
                                        integCntDic[key] += dfMergeResultSfReset['미착공수량'][i]
                                    else:
                                        integCntDic[key] = dfMergeResultSfReset['미착공수량'][i]
                        for y in range(0, maxNormalCommaCnt + 1):
                            if str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != '' and str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != 'nan':
                                if key == float(dfMergeResultSfReset['일반사양' + str(y + 1)][i]):
                                    if key in integCntDic:
                                        integCntDic[key] += dfMergeResultSfReset['미착공수량'][i]
                                    else:
                                        integCntDic[key] = dfMergeResultSfReset['미착공수량'][i]
                    if dfMergeResultSfReset['남은 워킹데이'][i] == 0:
                        workDay = 1
                    else:
                        workDay = dfMergeResultSfReset['남은 워킹데이'][i]
                    for x in range(0, maxSpCommaCnt + 1):
                        if str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != '' and str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != 'nan':
                            limitCopyCnt = capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])]
                            if (math.ceil(limitCopyCnt) <= (integCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] / workDay)) and copyCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] > 0:
                                if len(minContCntDic) > 0:
                                    if dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])]['그룹명'].values[0] in minContCntDic:
                                        if minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])]['그룹명'].values[0]][0] < (integCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] / workDay):
                                            minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])]['그룹명'].values[0]][0] = (integCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] / workDay)
                                            minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])]['그룹명'].values[0]][1] = int(dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])]['日(LINE)가능대수'].values[0])
                                            minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])]['그룹명'].values[0]][2] = dfMergeResultSfReset['Planned Prod. Completion date'][i]
                                else:
                                    minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])]['그룹명'].values[0]] = [(integCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] / workDay),
                                                                                                                                                                int(dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])]['日(LINE)가능대수'].values[0]),
                                                                                                                                                                dfMergeResultSfReset['Planned Prod. Completion date'][i]]
                            limitCnt = capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] * float(dfMergeResultSfReset['착공비율(%)'][i])
                            if (math.ceil(limitCnt) <= (integCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] / workDay)) and preOrderDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] > 0:
                                for j in dfMergeResultSfReset.index:
                                    if dfMergeResultSfReset['특수사양' + str(x + 1)][i] in dfMergeResultSfReset['특수사양'][j]:
                                        if preOrderDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] > 0:
                                            dfMergeResultSfReset['선행착공대상'][j] = '대상'
                                            preOrderDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] -= dfMergeResultSfReset['착공수량'][j]
                                        else:
                                            break
                    for y in range(0, maxNormalCommaCnt + 1):
                        if str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != '' and str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != 'nan':
                            limitCopyCnt = capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])]
                            if (math.ceil(limitCopyCnt) <= (integCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] / workDay)) and copyCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] > 0:
                                if len(minContCntDic) > 0:
                                    if dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])]['그룹명'].values[0] in minContCntDic:
                                        if minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])]['그룹명'].values[0]][0] < (integCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] / workDay):
                                            minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])]['그룹명'].values[0]][0] = (integCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] / workDay)
                                            minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])]['그룹명'].values[0]][1] = int(dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])]['日(LINE)가능대수'].values[0])
                                            minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])]['그룹명'].values[0]][2] = dfMergeResultSfReset['Planned Prod. Completion date'][i]
                                else:
                                    minContCntDic[dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])]['그룹명'].values[0]] = [(integCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] / workDay),
                                                                                                                                                                int(dfCondition[dfCondition['No'] == float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])]['日(LINE)가능대수'].values[0]),
                                                                                                                                                                dfMergeResultSfReset['Planned Prod. Completion date'][i]]
                            limitCnt = capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] * float(dfMergeResultSfReset['착공비율(%)'][i])
                            if (math.ceil(limitCnt) <= (integCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] / workDay)) and preOrderDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] > 0:
                                for j in dfMergeResultSfReset.index:
                                    if str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) in str(dfMergeResultSfReset['일반사양'][j]):
                                        if preOrderDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] > 0:
                                            dfMergeResultSfReset['선행착공대상'][j] = '대상'
                                            preOrderDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] -= dfMergeResultSfReset['착공수량'][j]
                                        else:
                                            break
                if len(minContCntDic) > 0:
                    for key, value in minContCntDic.items():
                        logging.warning('「%s」 사양이 「완성지정일: %s」 까지 현재 「일 가능대수: %i 대」로는 착공량 부족이 예상됩니다. 최소 필요 착공량은 「%i 대」 입니다.',
                                                key,
                                                str(value[2]),
                                                value[1],
                                                math.ceil(value[0]))
                if self.isDebug:
                    dfMergeResultSfReset.to_excel('.\\debug\\UTA\\flow10-2.xlsx')

                dfMergeResultSfReset = dfMergeResultSfReset.sort_values(by=['긴급오더', 'MAX 착공 필요', '선행착공대상', 'Planned Prod. Completion date', 'Planned Shipping date', '특수사양', '일반사양'],
                                                                            ascending=[False, False, False, True, True, True, True])
                # 인덱스 재설정
                dfMergeResultSfReset = dfMergeResultSfReset.reset_index(level=None, drop=False, inplace=False)
                # 이전 인덱스 삭제
                dfMergeResultSfReset.drop('index', axis=1, inplace=True)

                if self.isDebug:
                    dfMergeResultSfReset.to_excel('.\\debug\\UTA\\flow10-3.xlsx')
                self.progressbar.setRange(0, dfMergeResultSfReset.index[-1])
                limitCnt = constructTempCnt / 2
                # 착공량 분배 로직
                for i in dfMergeResultSfReset.index:
                    self.progressbar.setValue(i / 2)
                    if i == 157:
                        print()
                    if constructTempCnt > 0:
                        if dfMergeResultSfReset['홀딩오더'][i] != '대상':
                            if dfMergeResultSfReset['긴급오더'][i] != '대상':
                                tempMinCnt = constructTempCnt
                                # 특수, 일반을 제외한 대상을 구별하기 위한 플래그
                                normalFlag = True
                                # UT32A, UT52A, UM33A 대상일 경우의 로직
                                if limitFlag and (str(dfMergeResultSfReset['UT32A, UT52A, UM33A구분여부'][i]) != '' and str(dfMergeResultSfReset['UT32A, UT52A, UM33A구분여부'][i]) != 'nan'):
                                    if limitCnt <= tempMinCnt:
                                        tempMinCnt = limitCnt
                                # 대상의 미착공수량을 확인
                                if float(dfMergeResultSfReset['미착공수량'][i]) * float(dfMergeResultSfReset['공수배율'][i]) <= tempMinCnt:
                                    tempMinCnt = float(dfMergeResultSfReset['미착공수량'][i]) * float(dfMergeResultSfReset['공수배율'][i])
                                # 특수사양의 남은 일 가능대수 확인
                                for x in range(0, maxSpCommaCnt + 1):
                                    if str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != '' and str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != 'nan':
                                        normalFlag = False
                                        if capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] * float(dfMergeResultSfReset['공수배율'][i]) <= tempMinCnt:
                                            tempMinCnt = capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] * float(dfMergeResultSfReset['공수배율'][i])
                                # 일반사양의 남은 일 가능대수 확인
                                for y in range(0, maxNormalCommaCnt + 1):
                                    if str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != '' and str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != 'nan':
                                        normalFlag = False
                                        if capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] * float(dfMergeResultSfReset['공수배율'][i]) <= tempMinCnt:
                                            tempMinCnt = capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] * float(dfMergeResultSfReset['공수배율'][i])
                                # 착공 여유분이 있을 때의 로직
                                if tempMinCnt > 0:
                                    dfMergeResultSfReset['착공수량'][i] = math.ceil(tempMinCnt / float(dfMergeResultSfReset['공수배율'][i]))
                                    # 특수 or 일반 사양일 경우
                                    if not normalFlag:
                                        for x in range(0, maxSpCommaCnt + 1):
                                            if str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != '' and str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != 'nan':
                                                if capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] - dfMergeResultSfReset['착공수량'][i] >= 0:
                                                    capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] -= dfMergeResultSfReset['착공수량'][i]
                                                else:
                                                    capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] = 0
                                        for y in range(0, maxNormalCommaCnt + 1):
                                            if str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != '' and str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != 'nan':
                                                if capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] - dfMergeResultSfReset['착공수량'][i] >= 0:
                                                    capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] -= dfMergeResultSfReset['착공수량'][i]
                                                else:
                                                    capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] = 0
                                    # UT32A, UT52A, UM33A 대상일 경우는 제한Cnt에서도 제외
                                    if (str(dfMergeResultSfReset['UT32A, UT52A, UM33A구분여부'][i]) != '' and str(dfMergeResultSfReset['UT32A, UT52A, UM33A구분여부'][i]) != 'nan'):
                                        if limitCnt > 0:
                                            limitCnt -= dfMergeResultSfReset['착공수량'][i] * float(dfMergeResultSfReset['공수배율'][i])
                                            constructTempCnt -= dfMergeResultSfReset['착공수량'][i] * float(dfMergeResultSfReset['공수배율'][i])
                                            dfCopy = dfCopy.append(dfMergeResultSfReset.iloc[i])
                                    else:
                                        constructTempCnt -= dfMergeResultSfReset['착공수량'][i] * float(dfMergeResultSfReset['공수배율'][i])
                                        dfCopy = dfCopy.append(dfMergeResultSfReset.iloc[i])
                            # 긴급 오더 일 경우
                            else:
                                dfMergeResultSfReset['착공수량'][i] = dfMergeResultSfReset['미착공수량'][i]
                                # 특수사양의 남은 일 가능대수 확인 (확인 및 빼기만 할 뿐, 착공수량 입력에는 영향없음)
                                for x in range(0, maxSpCommaCnt + 1):
                                    if str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != '' and str(dfMergeResultSfReset['특수사양' + str(x + 1)][i]) != 'nan':
                                        if capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] - float(dfMergeResultSfReset['착공수량'][i]) >= 0:
                                            capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] -= float(dfMergeResultSfReset['착공수량'][i])
                                        else:
                                            capableCntDic[float(dfMergeResultSfReset['특수사양' + str(x + 1)][i])] = 0
                                # 일반사양의 남은 일 가능대수 확인 (확인 및 빼기만 할 뿐, 착공수량 입력에는 영향없음)
                                for y in range(0, maxNormalCommaCnt + 1):
                                    if str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != '' and str(dfMergeResultSfReset['일반사양' + str(y + 1)][i]) != 'nan':
                                        if capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] - float(dfMergeResultSfReset['착공수량'][i]) >= 0:
                                            capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] -= float(dfMergeResultSfReset['착공수량'][i])
                                        else:
                                            capableCntDic[float(dfMergeResultSfReset['일반사양' + str(y + 1)][i])] = 0
                                # UT32A, UT52A, UM33A 대상일 경우, 제한Cnt에서 빼기 (확인 및 빼기만 할 뿐, 착공수량 입력에는 영향없음)
                                if (str(dfMergeResultSfReset['UT32A, UT52A, UM33A구분여부'][i]) != '' and str(dfMergeResultSfReset['UT32A, UT52A, UM33A구분여부'][i]) != 'nan'):
                                    limitCnt -= float(dfMergeResultSfReset['착공수량'][i]) * float(dfMergeResultSfReset['공수배율'][i])
                                constructTempCnt -= float(dfMergeResultSfReset['착공수량'][i]) * float(dfMergeResultSfReset['공수배율'][i])
                                dfCopy = dfCopy.append(dfMergeResultSfReset.iloc[i])
                    else:
                        self.progressbar.setValue(dfMergeResultSfReset.index[-1] / 2)
                        break
                # 디버그용 파일 출력
                if self.isDebug:
                    dfCopy.to_excel('.\\debug\\UTA\\flow11.xlsx')
                dfCopy['착공수량*공수'] = dfCopy['착공수량'] * dfCopy['공수배율']
                # 디버그용 파일 출력
                if self.isDebug:
                    dfCopy.to_excel('.\\debug\\UTA\\flow12.xlsx')
                # 최소 사이클 = 4
                cycleStd = 4
                totalOrderCntDic = {}
                cycleStdDic = {}
                # 사이클 기준을 결정하기 위한 로직
                for i in dfCopy.index:
                    if str(dfCopy['특수사양'][i]) != '' and str(dfCopy['특수사양'][i]) != 'nan':
                        for j in range(1, maxSpCommaCnt + 2):
                            # 특수사양만 있는 경우
                            if str(dfCopy['특수사양' + str(j)][i]) != 'nan' and str(dfCopy['특수사양' + str(j)][i]) != '':
                                if dfCopy['특수사양' + str(j)][i] in totalOrderCntDic:
                                    totalOrderCntDic[dfCopy['특수사양' + str(j)][i]] += dfCopy['착공수량'][i]
                                else:
                                    totalOrderCntDic[dfCopy['특수사양' + str(j)][i]] = dfCopy['착공수량'][i]
                                    cycleStdDic[dfCopy['특수사양' + str(j)][i]] = dfCopy['Cycle기준'][i]
                                # normalSpec_flag = False
                        for j in range(1, maxNormalCommaCnt + 2):
                            # 특수사양 + 일반사양 있는 경우
                            if str(dfCopy['일반사양' + str(j)][i]) != 'nan' and str(dfCopy['일반사양' + str(j)][i]) != '':
                                # normalSpec_flag = False
                                if j == 1:
                                    if dfCopy['일반사양' + str(j)][i] in totalOrderCntDic:
                                        totalOrderCntDic[dfCopy['일반사양' + str(j)][i]] += dfCopy['착공수량'][i]
                                    else:
                                        totalOrderCntDic[dfCopy['일반사양' + str(j)][i]] = dfCopy['착공수량'][i]
                                        cycleStdDic[dfCopy['일반사양' + str(j)][i]] = dfCopy['Cycle기준'][i]
                    # 일반 사양만 있는 경우,
                    elif str(dfCopy['일반사양'][i]) != '' and str(dfCopy['일반사양'][i]) != 'nan':
                        for j in range(1, maxNormalCommaCnt + 2):
                            if str(dfCopy['일반사양' + str(j)][i]) != 'nan' and str(dfCopy['일반사양' + str(j)][i]) != '':
                                # normalSpec_flag = False
                                if j == 1:
                                    if dfCopy['일반사양' + str(j)][i] in totalOrderCntDic:
                                        totalOrderCntDic[dfCopy['일반사양' + str(j)][i]] += dfCopy['착공수량'][i]
                                    else:
                                        totalOrderCntDic[dfCopy['일반사양' + str(j)][i]] = dfCopy['착공수량'][i]
                                        cycleStdDic[dfCopy['일반사양' + str(j)][i]] = dfCopy['Cycle기준'][i]
                    # 특수사양, 일반사양 모두 해당 없는 경우
                    elif str(dfCopy['일반사양'][i]) == '' or str(dfCopy['일반사양'][i]) == 'nan':
                        if '그외' in totalOrderCntDic:
                            totalOrderCntDic['그외'] += dfCopy['착공수량'][i]
                        else:
                            totalOrderCntDic['그외'] = dfCopy['착공수량'][i]
                            cycleStdDic['그외'] = dfCopy['Cycle기준'][i]
                for key_i, value_i in cycleStdDic.items():
                    if value_i != 0 and str(value_i) != 'nan':
                        for key_j, value_j in totalOrderCntDic.items():
                            if key_i == key_j:
                                if math.ceil(value_j / value_i) > cycleStd:
                                    cycleStd = math.ceil(value_j / value_i)
                # Linkage Number 기준으로 병합
                dfCopy = dfCopy.astype({'Linkage Number': 'str'})
                dfLevelingResult = dfLevelingResult.astype({'Linkage Number': 'str'})
                dfMergeOrder = pd.merge(dfCopy, dfLevelingResult, on='Linkage Number', how='left')
                # 디버그용 파일 출력
                if self.isDebug:
                    dfMergeOrder.to_excel('.\\debug\\UTA\\flow13.xlsx')
                # 인덱스 재설정
                dfMergeOrderResult = pd.DataFrame().reindex_like(dfMergeOrder)
                dfMergeOrderResult = dfMergeOrderResult[0:0]
                self.progressbar.setRange(0, (dfMergeResultSfReset.index[-1] / 2) + dfCopy.index[-1])
                # 개별 착공으로 분할
                for i in dfCopy.index:
                    self.progressbar.setValue((dfMergeResultSfReset.index[-1] / 2) + i)
                    for j in dfMergeOrder.index:
                        if dfCopy['Linkage Number'][i] == dfMergeOrder['Linkage Number'][j]:
                            if j > 0:
                                if dfMergeOrder['Linkage Number'][j] != dfMergeOrder['Linkage Number'][j - 1]:
                                    orderCnt = int(dfCopy['착공수량'][i])
                            else:
                                orderCnt = int(dfCopy['착공수량'][i])
                            if orderCnt > 0:
                                dfMergeOrderResult = dfMergeOrderResult.append(dfMergeOrder.iloc[j])
                                orderCnt -= 1
                            # total_orderCnt -= 1
                # 인덱스 재설정
                dfMergeOrderResult.reset_index(drop=True)
                # 디버그용 파일 출력
                if self.isDebug:
                    dfMergeOrderResult.to_excel('.\\debug\\UTA\\flow14.xlsx')
                # 사이클 순환을 위한 변수 선언
                cycleList = []
                cycleOrderCntDic = {}
                dfMergeOrderResult['사이클그룹'] = 0
                for key, value in totalOrderCntDic.items():
                    cycleOrderCntDic[key] = math.ceil(value / cycleStd)
                for i in range(0, cycleStd):
                    cycleList.append(cycleOrderCntDic.copy())
                for i in dfMergeOrderResult.index:
                    for j in range(0, cycleStd):
                        normalSpec = ""
                        # print(str(dfMergeOrderResult['특수사양'][i]))
                        if str(dfMergeOrderResult['긴급오더'][i]) == '대상' or len(str(dfMergeOrderResult['특수사양'][i])) != 0:
                            dfMergeOrderResult['사이클그룹'][i] = 0
                        else:
                            if str(dfMergeOrderResult['일반사양1'][i]) != 'nan' and str(dfMergeOrderResult['일반사양1'][i]) != '':
                                normalSpec = dfMergeOrderResult['일반사양1'][i]
                            elif str(dfMergeOrderResult['일반사양1'][i]) == 'nan' and str(dfMergeOrderResult['특수사양1'][i]) == 'nan':
                                normalSpec = '그외'
                            elif str(dfMergeOrderResult['일반사양1'][i]) == '' and str(dfMergeOrderResult['특수사양1'][i]) == '':
                                normalSpec = '그외'
                            if normalSpec != "" and cycleList[j][normalSpec] != 0:
                                dfMergeOrderResult['사이클그룹'][i] = j + 1
                                cycleList[j][normalSpec] -= 1
                                break
                dfMergeOrderResult = dfMergeOrderResult.sort_values(by=['긴급오더', '사이클그룹'], ascending=[False, True])
                dfMergeOrderResult = dfMergeOrderResult.sort_values(by=['긴급오더', '사이클그룹', '특수사양', 'MS-CODE', 'Linkage Number', 'Planned Prod. Completion date', '일반사양'],
                                                                        ascending=[False, True, True, True, True, True, True])
                dfMergeOrderResult = dfMergeOrderResult.reset_index(drop=True)
                dfMergeOrderResult['No (*)'] = (dfMergeOrderResult.index.astype(int) + 1) * 10
                dfMergeOrderResult['Scheduled Start Date (*)'] = self.labelDate.text()
                dfMergeOrderResult['Planned Order'] = dfMergeOrderResult['Planned Order'].astype(int).astype(str).str.zfill(10)
                dfMergeOrderResult['Scheduled End Date'] = dfMergeOrderResult['Scheduled End Date'].astype(str).str.zfill(10)
                dfMergeOrderResult['Specified Start Date'] = dfMergeOrderResult['Specified Start Date'].astype(str).str.zfill(10)
                dfMergeOrderResult['Specified End Date'] = dfMergeOrderResult['Specified End Date'].astype(str).str.zfill(10)
                dfMergeOrderResult['Spec Freeze Date'] = dfMergeOrderResult['Spec Freeze Date'].astype(str).str.zfill(10)
                dfMergeOrderResult['Component Number'] = dfMergeOrderResult['Component Number'].astype(int).astype(str).str.zfill(4)

                # 디버그용 파일 출력
                if self.isDebug:
                    dfMergeOrderResult.to_excel('.\\debug\\UTA\\flow15.xlsx')
                dfMergeOrderResult = dfMergeOrderResult[['No (*)',
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
                # 디버그용 파일 출력
                if self.isDebug:
                    dfMergeOrderResult.to_excel('.\\debug\\UTA\\flow16.xlsx')
                # 최종결과물 출력
                outputFile = '.\\result\\UTA\\5400_A0100A81_' + today + '_Leveling_List.xlsx'
                dfMergeOrderResult.to_excel(outputFile, index=False)
                logging.info('결과물이 %s 파일로 정상적으로 출력되었습니다.', outputFile)
                self.runBtn.setEnabled(True)
            elif int(self.maxOrderinput.text()) == 0:
                logging.warning('최대 착공량이 0입니다. 다시 입력해주세요.')
                self.runBtn.setEnabled(True)
            elif len(self.maxOrderinput.text()) == 0:
                logging.warning('최대 착공량이 입력되지 않았습니다.')
                self.runBtn.setEnabled(True)
        except Exception as e:
            logging.exception(e, exc_info=True)
            self.runBtn.setEnabled(True)
