import datetime
import glob
import logging
from logging.handlers import RotatingFileHandler
import os
from PyQt5.QtCore import Qt, QCoreApplication
from PyQt5.QtGui import QStandardItemModel, QIcon, QIntValidator
from PyQt5.QtWidgets import QMainWindow, QProgressBar, QWidget, QGridLayout, QGroupBox, QLineEdit, QSizePolicy, QToolButton, QLabel, QFrame, QListView, QMenuBar, QStatusBar, QPushButton, QComboBox
from PyQt5.QtCore import pyqtSlot, QThread, QRect, QSize
import pandas as pd
from QPlainTextEditLogger import QPlainTextEditLogger
from CustomFormatter import CustomFormatter
from CalendarWindow import CalendarWindow
from UI_SubWindow import UI_SubWindow
from Fam3MainThread import Fam3MainThread
from Fam3PowerThread import Fam3PowerThread
from Fam3SpThread import Fam3SpThread


class Ui_Fam3MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi()

    def setupUi(self):
        rfh = RotatingFileHandler(filename='./Log.log', mode='a', maxBytes=5 * 1024 * 1024, backupCount=2, encoding=None, delay=0)
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%m/%d/%Y %H:%M:%S', handlers=[rfh])
        self.setObjectName('MainWindow')
        self.resize(900, 1000)
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
        self.label_round = QLabel(self.groupBox)
        self.label_round.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label_round.setObjectName('label4')
        self.gridLayout3.addWidget(self.label_round, 0, 0, 1, 1)
        self.mainOrderinput = QLineEdit(self.groupBox)
        self.mainOrderinput.setMinimumSize(QSize(0, 25))
        self.mainOrderinput.setObjectName('mainOrderinput')
        self.mainOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.mainOrderinput, 1, 1, 1, 1)
        self.spModuleOrderinput = QLineEdit(self.groupBox)
        self.spModuleOrderinput.setMinimumSize(QSize(0, 25))
        self.spModuleOrderinput.setObjectName('spModuleOrderinput')
        self.spModuleOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.spModuleOrderinput, 2, 1, 1, 1)
        self.spNonModuleOrderinput = QLineEdit(self.groupBox)
        self.spNonModuleOrderinput.setMinimumSize(QSize(0, 25))
        self.spNonModuleOrderinput.setObjectName('spModuleOrderinput')
        self.spNonModuleOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.spNonModuleOrderinput, 3, 1, 1, 1)
        self.powerOrderinput = QLineEdit(self.groupBox)
        self.powerOrderinput.setMinimumSize(QSize(0, 25))
        self.powerOrderinput.setObjectName('powerOrderinput')
        self.powerOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.powerOrderinput, 4, 1, 1, 1)
        self.dateBtn = QToolButton(self.groupBox)
        self.dateBtn.setMinimumSize(QSize(0, 25))
        self.dateBtn.setObjectName('dateBtn')
        self.gridLayout3.addWidget(self.dateBtn, 5, 1, 1, 1)
        self.emgFileInputBtn = QPushButton(self.groupBox)
        self.emgFileInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.emgFileInputBtn, 6, 1, 1, 1)
        self.holdFileInputBtn = QPushButton(self.groupBox)
        self.holdFileInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.holdFileInputBtn, 9, 1, 1, 1)
        self.label4 = QLabel(self.groupBox)
        self.label4.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label4.setObjectName('label4')
        self.gridLayout3.addWidget(self.label4, 7, 1, 1, 1)
        self.label5 = QLabel(self.groupBox)
        self.label5.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label5.setObjectName('label5')
        self.gridLayout3.addWidget(self.label5, 7, 2, 1, 1)
        self.label6 = QLabel(self.groupBox)
        self.label6.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label6.setObjectName('label6')
        self.gridLayout3.addWidget(self.label6, 10, 1, 1, 1)
        self.label7 = QLabel(self.groupBox)
        self.label7.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label7.setObjectName('label7')
        self.gridLayout3.addWidget(self.label7, 10, 2, 1, 1)
        listViewModelEmgLinkage = QStandardItemModel()
        self.listViewEmgLinkage = QListView(self.groupBox)
        self.listViewEmgLinkage.setModel(listViewModelEmgLinkage)
        self.gridLayout3.addWidget(self.listViewEmgLinkage, 8, 1, 1, 1)
        listViewModelEmgmscode = QStandardItemModel()
        self.listViewEmgmscode = QListView(self.groupBox)
        self.listViewEmgmscode.setModel(listViewModelEmgmscode)
        self.gridLayout3.addWidget(self.listViewEmgmscode, 8, 2, 1, 1)
        listViewModelHoldLinkage = QStandardItemModel()
        self.listViewHoldLinkage = QListView(self.groupBox)
        self.listViewHoldLinkage.setModel(listViewModelHoldLinkage)
        self.gridLayout3.addWidget(self.listViewHoldLinkage, 11, 1, 1, 1)
        listViewModelHoldmscode = QStandardItemModel()
        self.listViewHoldmscode = QListView(self.groupBox)
        self.listViewHoldmscode.setModel(listViewModelHoldmscode)
        self.gridLayout3.addWidget(self.listViewHoldmscode, 11, 2, 1, 1)
        self.labelBlank = QLabel(self.groupBox)
        self.labelBlank.setObjectName('labelBlank')
        self.gridLayout3.addWidget(self.labelBlank, 4, 4, 1, 1)
        self.progressbar_main = QProgressBar(self.groupBox)
        self.progressbar_main.setObjectName('progressbar_main')
        self.progressbar_main.setAlignment(Qt.AlignVCenter)
        self.progressbar_main.setFormat('메인라인 진행률')
        self.gridLayout3.addWidget(self.progressbar_main, 12, 1, 1, 2)
        self.progressbar_sp = QProgressBar(self.groupBox)
        self.progressbar_sp.setObjectName('progressbar_sp')
        self.progressbar_sp.setAlignment(Qt.AlignVCenter)
        self.progressbar_sp.setFormat('특수라인 진행률')
        self.gridLayout3.addWidget(self.progressbar_sp, 13, 1, 1, 2)
        self.progressbar_power = QProgressBar(self.groupBox)
        self.progressbar_power.setObjectName('progressbar_power')
        self.progressbar_power.setAlignment(Qt.AlignVCenter)
        self.progressbar_power.setFormat('전원라인 진행률')
        self.gridLayout3.addWidget(self.progressbar_power, 14, 1, 1, 2)
        self.runBtn = QToolButton(self.groupBox)
        sizePolicy = QSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.runBtn.sizePolicy().hasHeightForWidth())
        self.runBtn.setSizePolicy(sizePolicy)
        self.runBtn.setMinimumSize(QSize(30, 35))
        self.runBtn.setStyleSheet('background-color: rgb(63, 63, 63);\ncolor: rgb(255, 255, 255);')
        self.runBtn.setObjectName('runBtn')
        self.gridLayout3.addWidget(self.runBtn, 16, 3, 1, 2)
        self.label = QLabel(self.groupBox)
        self.label.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label.setObjectName('label')
        self.gridLayout3.addWidget(self.label, 1, 0, 1, 1)
        self.label9 = QLabel(self.groupBox)
        self.label9.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label9.setObjectName('label9')
        self.gridLayout3.addWidget(self.label9, 2, 0, 1, 1)
        self.label10 = QLabel(self.groupBox)
        self.label10.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label10.setObjectName('label10')
        self.gridLayout3.addWidget(self.label10, 4, 0, 1, 1)
        self.label19 = QLabel(self.groupBox)
        self.label19.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label19.setObjectName('label19')
        self.gridLayout3.addWidget(self.label19, 3, 0, 1, 1)
        self.label12 = QLabel(self.groupBox)
        self.label12.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label12.setObjectName('label12')
        self.gridLayout3.addWidget(self.label12, 2, 2, 1, 1)
        self.label13 = QLabel(self.groupBox)
        self.label13.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label13.setObjectName('label13')
        self.gridLayout3.addWidget(self.label13, 3, 2, 1, 1)
        self.label8 = QLabel(self.groupBox)
        self.label8.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label8.setObjectName('label8')
        self.gridLayout3.addWidget(self.label8, 5, 0, 1, 1)
        self.labelDate = QLabel(self.groupBox)
        self.labelDate.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.labelDate.setObjectName('labelDate')
        self.gridLayout3.addWidget(self.labelDate, 5, 2, 1, 1)
        self.label2 = QLabel(self.groupBox)
        self.label2.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label2.setObjectName('label2')
        self.gridLayout3.addWidget(self.label2, 6, 0, 1, 1)
        self.label3 = QLabel(self.groupBox)
        self.label3.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label3.setObjectName('label3')
        self.gridLayout3.addWidget(self.label3, 9, 0, 1, 1)
        self.line = QFrame(self.groupBox)
        self.line.setFrameShape(QFrame.HLine)
        self.line.setFrameShadow(QFrame.Sunken)
        self.line.setObjectName('line')
        self.cb_round = QComboBox(self.groupBox)
        self.gridLayout3.addWidget(self.cb_round, 0, 1, 1, 1)
        self.gridLayout3.addWidget(self.line, 15, 0, 1, 10)
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
        # 디버그용 플래그
        self.isDebug = True
        self.isFileReady = False
        self.MaxOrderInputFilePath = r'.\\input\\FAM3\\1차_착공량입력.xlsx'
        self.etcOrderInputFilePath = r'.\\input\\FAM3\\2차_착공량입력.xlsx'
        self.df_etcOrderInput = self.readMaxOrderFile()
        self.cb_round.currentTextChanged.connect(self.readMaxOrderFile)
        self.dict_mainEmgLinkage = {}
        self.dict_mainEmgMsCode = {}
        self.dict_powerEmgLinkage = {}
        self.dict_powerEmgMsCode = {}
        self.dict_spEmgLinkage = {}
        self.dict_spEmgMsCode = {}
        self.isMainEnd = False
        self.isPowerEnd = False
        self.isSpEnd = False
        if self.isDebug:
            self.date = QLineEdit(self.groupBox)
            self.date.setObjectName('date')
            self.gridLayout3.addWidget(self.date, 12, 0, 1, 1)
            self.date.setPlaceholderText('디버그용 날짜입력')
        self.thread = QThread()
        self.thread.setTerminationEnabled(True)
        self.thread2 = QThread()
        self.thread2.setTerminationEnabled(True)
        self.thread3 = QThread()
        self.thread3.setTerminationEnabled(True)
        self.show()

    def retranslateUi(self, MainWindow):
        _translate = QCoreApplication.translate
        MainWindow.setWindowTitle(_translate('MainWindow', 'FA-M3 착공 평준화 자동화 프로그램 Rev1.02'))
        MainWindow.setWindowIcon(QIcon('.\\Logo\\logo.png'))
        self.label_round.setText(_translate('MainWindow', '착공 회차 선택:'))
        self.label.setText(_translate('MainWindow', '메인 생산대수:'))
        self.label9.setText(_translate('MainWindow', '특수(모듈) 생산대수:'))
        self.label19.setText(_translate('MainWindow', '특수(비모듈) 생산대수:'))
        self.label10.setText(_translate('MainWindow', '전원 생산대수:'))
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
        list_round = ['1차', '2차']
        self.cb_round.addItems(list_round)
        logging.info('프로그램이 정상 기동했습니다')

    # 최대착공량입력 파일 불러오기 함수
    def readMaxOrderFile(self):
        if self.cb_round.currentText() == "1차":
            self.MaxOrderInputFilePath = r'.\\input\\FAM3\\1차_착공량입력.xlsx'
            self.etcOrderInputFilePath = r'.\\input\\FAM3\\2차_착공량입력.xlsx'
        elif self.cb_round.currentText() == "2차":
            self.MaxOrderInputFilePath = r'.\\input\\FAM3\\2차_착공량입력.xlsx'
            self.etcOrderInputFilePath = r'.\\input\\FAM3\\1차_착공량입력.xlsx'
        if os.path.exists(self.MaxOrderInputFilePath):
            df_orderInput = pd.read_excel(self.MaxOrderInputFilePath)
            self.mainOrderinput.setText(str(df_orderInput['착공량'][0]))
            self.spModuleOrderinput.setText(str(df_orderInput['착공량'][1]))
            self.spNonModuleOrderinput.setText(str(df_orderInput['착공량'][2]))
            self.powerOrderinput.setText(str(df_orderInput['착공량'][3]))
        else:
            logging.error('%s 파일이 없습니다. 착공량을 수동으로 입력해주세요.', self.MaxOrderInputFilePath)
        if os.path.exists(self.etcOrderInputFilePath):
            df_etcOrderInput = pd.read_excel(self.etcOrderInputFilePath)
        else:
            df_etcOrderInput = pd.DataFrame()
            logging.error('%s 파일이 없습니다. 파일을 확인해주세요.', self.etcOrderInputFilePath)
        return df_etcOrderInput

    # 착공지정일 캘린더 호출
    def selectStartDate(self):
        self.calendar_window = CalendarWindow()
        self.calendar_window.submitClicked.connect(self.getDate)

    def getDate(self, date):
        if len(date) > 0:
            self.labelDate.setText(date)
            logging.info('착공지정일이 %s 로 정상적으로 지정되었습니다.', date)
        else:
            logging.error('착공지정일이 선택되지 않았습니다.')

    # 긴급오더 윈도우 호출
    @pyqtSlot()
    def emgWindow(self):
        list_emgHold = self.loadEmgHoldList()
        self.sub_window = UI_SubWindow(list_emgHold[0], list_emgHold[1])
        self.sub_window.submitClicked.connect(self.getEmgListview)

    # 홀딩오더 윈도우 호출
    @pyqtSlot()
    def holdWindow(self):
        list_emgHold = self.loadEmgHoldList()
        self.sub_window = UI_SubWindow(list_emgHold[2], list_emgHold[3])
        self.sub_window.submitClicked.connect(self.getHoldListview)

    # 긴급오더 리스트뷰 가져오기
    def getEmgListview(self, list):
        if len(list) > 0:
            self.listViewEmgLinkage.setModel(list[0])
            self.listViewEmgmscode.setModel(list[1])
            logging.info('긴급오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('긴급오더 리스트가 없습니다. 다시 한번 확인해주세요')

    # 홀딩오더 리스트뷰 가져오기
    def getHoldListview(self, list):
        if len(list) > 0:
            self.listViewHoldLinkage.setModel(list[0])
            self.listViewHoldmscode.setModel(list[1])
            logging.info('홀딩오더 리스트를 정상적으로 불러왔습니다.')
        else:
            logging.error('홀딩오더 리스트가 없습니다. 다시 한번 확인해주세요')

    # 착공지정일 가져오기
    def getStartDate(self, date):
        if len(date) > 0:
            self.labelDate.setText(date)
            logging.info('착공지정일이 %s 로 정상적으로 지정되었습니다.', date)
        else:
            logging.error('착공지정일이 선택되지 않았습니다.')

    # 실행버튼 활성화
    def enableRunBtn(self):
        self.runBtn.setEnabled(True)
        self.runBtn.setText('실행')

    # 실행버튼 비활성화
    def disableRunBtn(self):
        self.runBtn.setEnabled(False)
        self.runBtn.setText('실행 중')

    # 메인라인 에러메시지 출력용 함수
    def mainShowError(self, str):
        logging.error(f'메인라인 에러 - {str}')
        self.enableRunBtn()
        self.progressbar_main.setValue(0)
        self.thread.quit()
        self.thread.wait()

    # 전원라인 에러메시지 출력용 함수
    def powerShowError(self, str):
        logging.warning(f'전원라인 에러 - {str}')
        self.enableRunBtn()
        self.progressbar_power.setValue(0)
        self.thread2.quit()
        self.thread2.wait()

    # 특수라인 에러메시지 출력용 함수
    def spShowError(self, str):
        logging.warning(f'특수라인 에러 - {str}')
        self.enableRunBtn()
        self.progressbar_sp.setValue(0)
        self.thread3.quit()
        self.thread3.wait()

    # 메인라인 경고메시지 출력용 함수
    def mainShowWarning(self, str):
        logging.warning(f'메인라인 경고 - {str}')

    # 전원라인 경고메시지 출력용 함수
    def powerShowWarning(self, str):
        logging.warning(f'전원라인 경고 - {str}')

    # 특수라인 경고메시지 출력용 함수
    def spShowWarning(self, str):
        logging.warning(f'특수라인 경고 - {str}')

    # 메인라인 쓰레드 종료용 함수
    def mainThreadEnd(self, isEnd):
        if isEnd:
            logging.info('메인라인 착공이 완료되었습니다.')
            self.thread.quit()
            self.thread.wait()
        self.isMainEnd = isEnd
        if self.isMainEnd and self.isPowerEnd and self.isSpEnd:
            self.enableRunBtn()
            for key in self.dict_mainEmgLinkage.keys():
                if not (self.dict_mainEmgLinkage[key] or self.dict_powerEmgLinkage[key] or self.dict_spEmgLinkage[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')
            for key in self.dict_mainEmgMsCode.keys():
                if not (self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')

    # 전원라인 쓰레드 종료용 함수
    def powerThreadEnd(self, isEnd):
        if isEnd:
            logging.info('전원라인 착공이 완료되었습니다.')
            self.thread2.quit()
            self.thread2.wait()
        self.isPowerEnd = isEnd
        if self.isMainEnd and self.isPowerEnd and self.isSpEnd:
            self.enableRunBtn()
            for key in self.dict_mainEmgLinkage.keys():
                if not (self.dict_mainEmgLinkage[key] or self.dict_powerEmgLinkage[key] or self.dict_spEmgLinkage[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')
            for key in self.dict_mainEmgMsCode.keys():
                if not (self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')

    # 특수라인 쓰레드 종료용 함수
    def spThreadEnd(self, isEnd):
        if isEnd:
            logging.info('특수라인 착공이 완료되었습니다.')
            self.thread3.quit()
            self.thread3.wait()
        self.isSpEnd = isEnd
        if self.isMainEnd and self.isPowerEnd and self.isSpEnd:
            self.enableRunBtn()
            for key in self.dict_mainEmgLinkage.keys():
                if not (self.dict_mainEmgLinkage[key] or self.dict_powerEmgLinkage[key] or self.dict_spEmgLinkage[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')
            for key in self.dict_mainEmgMsCode.keys():
                if not (self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key] or self.dict_mainEmgMsCode[key]):
                    logging.warning(f'긴급오더 대상 : {str(key)} 가 모든 라인의 결과물에 없습니다.')

    def setMainEmgLinkage(self, dict_input):
        self.dict_mainEmgLinkage = dict_input

    def setMainEmgMscode(self, dict_input):
        self.dict_mainEmgMsCode = dict_input

    def setPowerEmgLinkage(self, dict_input):
        self.dict_powerEmgLinkage = dict_input

    def setPowerEmgMscode(self, dict_input):
        self.dict_powerEmgMsCode = dict_input

    def setSpEmgLinkage(self, dict_input):
        self.dict_spEmgLinkage = dict_input

    def setSpEmgMscode(self, dict_input):
        self.dict_spEmgMsCode = dict_input

    # 메인라인 프로그레스바 범위 설정용 함수
    def setMainMaxPb(self, maxPb):
        self.progressbar_main.setRange(0, maxPb)

    # 전원라인 프로그레스바 범위 설정용 함수
    def setPowerMaxPb(self, maxPb):
        self.progressbar_power.setRange(0, maxPb)

    # 특수라인 프로그레스바 범위 설정용 함수
    def setSpMaxPb(self, maxPb):
        self.progressbar_sp.setRange(0, maxPb)

    # 마스터파일 불러오기 함수
    def loadMasterFile(self):
        self.isFileReady = True
        masterFileList = []
        date = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            date = self.date.text()
        roundTxt = self.cb_round.currentText()
        sosFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\SOS2.xlsx'
        if float(self.mainOrderinput.text()) != 0.0:
            mainFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\MAIN.xlsx'
        else:
            mainFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.spModuleOrderinput.text()) != 0.0:
            spFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\OTHER.xlsx'
        else:
            spFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.powerOrderinput.text()) != 0.0:
            powerFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\POWER.xlsx'
        else:
            powerFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        calendarFilePath = r'.\\input\\FAM3\\Calendar_File\\FY' + date[2:4] + '_Calendar.xlsx'
        if os.path.exists(r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1311(' + date[4:8] + ')MAIN_2차.xlsx'):
            secMainListFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1311(' + date[4:8] + ')MAIN_2차.xlsx'
        else:
            secMainListFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        powerCondFilePath = r'.\\input\\FAM3\\DB\\Power\\FAM3_Power_MST_Table.xlsx'
        spCondFilePath = r'.\\input\\FAM3\\DB\\Sp\\FAM3_Sp_MST_Table.xlsx'
        smtAssyUnCheckFilePath = r'.\\input\\FAM3\\DB\\SP\\SMT수량_비관리대상.xlsx'
        if float(self.spNonModuleOrderinput.text()) != 0.0:
            if os.path.exists(r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\BL.xlsx'):
                nonSpBLFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\BL.xlsx'
            else:
                nonSpBLFilePath = r'.\\input\\FAM3\\Master_File\\' + date + r'\\'
        else:
            nonSpBLFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.spNonModuleOrderinput.text()) != 0.0:
            if os.path.exists(r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\TERMINAL.xlsx'):
                nonSpTerminalFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\TERMINAL.xlsx'
            else:
                nonSpTerminalFilePath = r'.\\input\\FAM3\\Master_File\\' + date + r'\\'
        else:
            nonSpTerminalFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.spNonModuleOrderinput.text()) != 0.0:
            if os.path.exists(r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\SLAVE.xlsx'):
                SpSlaveFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\SLAVE.xlsx'
            else:
                SpSlaveFilePath = r'.\\input\\FAM3\\Master_File\\' + date + r'\\'
        else:
            SpSlaveFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        mainAteCapaFilePath = r'.\\input\\FAM3\\DB\\Main\\' + roundTxt + '\\Main_ATE_Capacity_Table.xlsx'
        ctCondFilePath = r'.\\input\\FAM3\\DB\\CT\\FAM3_CT_MST_Table.xlsx'
        if os.path.exists(r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1313(' + date[4:8] + ')POWER_2차.xlsx'):
            secPowerListFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1313(' + date[4:8] + ')POWER_2차.xlsx'
        else:
            secPowerListFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if os.path.exists(r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1304(' + date[4:8] + ')OTHER_2차.xlsx'):
            secSpListFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\100L1304(' + date[4:8] + ')OTHER_2차.xlsx'
        else:
            secSpListFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        configFilePath = r'.\\Config.ini'
        holdingListFilePath = r'.\\input\\FAM3\\DB\\홀딩리스트.xlsx'
        # 위의 마스터파일 경로들을 리스트화
        pathList = [sosFilePath,
                    mainFilePath,
                    spFilePath,
                    powerFilePath,
                    calendarFilePath,
                    secMainListFilePath,
                    powerCondFilePath,
                    spCondFilePath,
                    smtAssyUnCheckFilePath,
                    nonSpBLFilePath,
                    nonSpTerminalFilePath,
                    SpSlaveFilePath,
                    mainAteCapaFilePath,
                    ctCondFilePath,
                    secPowerListFilePath,
                    secSpListFilePath,
                    configFilePath,
                    holdingListFilePath]
        # 각 경로의 파일들이 없는지 체크
        for path in pathList:
            if os.path.exists(path):
                file = glob.glob(path)[0]
                masterFileList.append(file)
            else:
                logging.error('%s 파일이 없습니다. 확인해주세요.', path)
                self.enableRunBtn()
                self.isFileReady = False
        if self.isFileReady:
            logging.info('마스터 파일 및 캘린더 파일을 정상적으로 불러왔습니다.')
        return masterFileList

    # 긴급/홀딩오더 리스트 불러오기
    def loadEmgHoldList(self):
        list_emgHold = []
        list_emgHold.append([str(self.listViewEmgLinkage.model().data(self.listViewEmgLinkage.model().index(x, 0))) for x in range(self.listViewEmgLinkage.model().rowCount())])
        list_emgHold.append([self.listViewEmgmscode.model().data(self.listViewEmgmscode.model().index(x, 0)) for x in range(self.listViewEmgmscode.model().rowCount())])
        list_emgHold.append([str(self.listViewHoldLinkage.model().data(self.listViewHoldLinkage.model().index(x, 0))) for x in range(self.listViewHoldLinkage.model().rowCount())])
        list_emgHold.append([self.listViewHoldmscode.model().data(self.listViewHoldmscode.model().index(x, 0)) for x in range(self.listViewHoldmscode.model().rowCount())])
        return list_emgHold

    # 특수라인 착공시작 함수
    def startSpLeveling(self, df):
        date = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            date = self.date.text()
        list_masterFile = self.loadMasterFile()
        list_emgHold = self.loadEmgHoldList()
        if self.isFileReady:
            if len(self.spModuleOrderinput.text()) > 0:
                if self.labelDate.text() != '미선택':
                    self.thread_sp = Fam3SpThread(self.isDebug,
                                                    date,
                                                    self.labelDate.text(),
                                                    list_masterFile,
                                                    float(self.spModuleOrderinput.text()),
                                                    float(self.spNonModuleOrderinput.text()),
                                                    list_emgHold,
                                                    df,
                                                    self.cb_round.currentText(),
                                                    self.df_etcOrderInput)
                    self.thread_sp.moveToThread(self.thread3)
                    self.thread3.started.connect(self.thread_sp.run)
                    self.thread_sp.spReturnError.connect(self.spShowError)
                    self.thread_sp.spReturnEnd.connect(self.spThreadEnd)
                    self.thread_sp.spReturnWarning.connect(self.spShowWarning)
                    self.thread_sp.spReturnMaxPb.connect(self.setSpMaxPb)
                    self.thread_sp.spReturnPb.connect(self.progressbar_sp.setValue)
                    self.thread_sp.spReturnEmgLinkage.connect(self.setSpEmgLinkage)
                    self.thread_sp.spReturnEmgMscode.connect(self.setSpEmgMscode)
                    self.thread3.start()
                else:
                    self.enableRunBtn()
                    logging.info('착공지정일이 입력되지 않았습니다. 캘린더로부터 착공지정일을 선택해주세요.')
            else:
                self.enableRunBtn()
                logging.info('특수기종 착공량이 입력되지 않아 특수기종 착공은 미실시 됩니다.')

    # 메인/전원라인 착공시작 함수
    @pyqtSlot()
    def startLeveling(self):
        self.disableRunBtn()
        self.setSpMaxPb(200)
        self.progressbar_sp.setValue(0)
        date = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            date = self.date.text()
        list_masterFile = self.loadMasterFile()
        list_emgHold = self.loadEmgHoldList()
        if self.isFileReady:
            if len(self.mainOrderinput.text()) > 0:
                if self.labelDate.text() != '미선택':
                    self.thread_main = Fam3MainThread(self.isDebug,
                                                        date,
                                                        self.labelDate.text(),
                                                        list_masterFile,
                                                        float(self.mainOrderinput.text()),
                                                        list_emgHold,
                                                        self.cb_round.currentText(),
                                                        self.df_etcOrderInput)
                    self.thread_main.moveToThread(self.thread)
                    self.thread.started.connect(self.thread_main.run)
                    self.thread_main.mainReturnError.connect(self.mainShowError)
                    self.thread_main.mainReturnEnd.connect(self.mainThreadEnd)
                    self.thread_main.mainReturnWarning.connect(self.mainShowWarning)
                    self.thread_main.mainReturnDf.connect(self.startSpLeveling)
                    self.thread_main.mainReturnMaxPb.connect(self.setMainMaxPb)
                    self.thread_main.mainReturnPb.connect(self.progressbar_main.setValue)
                    self.thread_main.mainReturnEmgLinkage.connect(self.setMainEmgLinkage)
                    self.thread_main.mainReturnEmgMscode.connect(self.setMainEmgMscode)
                    self.thread.start()
                else:
                    self.enableRunBtn()
                    logging.info('착공지정일이 입력되지 않았습니다. 캘린더로부터 착공지정일을 선택해주세요.')
            else:
                logging.info('메인기종 착공량이 입력되지 않아 메인기종 착공은 미실시 됩니다.')
            if len(self.powerOrderinput.text()) > 0:
                if self.labelDate.text() != '미선택':
                    self.thread_power = Fam3PowerThread(self.isDebug,
                                                        date,
                                                        self.labelDate.text(),
                                                        list_masterFile,
                                                        float(self.powerOrderinput.text()),
                                                        list_emgHold,
                                                        self.cb_round.currentText(),
                                                        self.df_etcOrderInput)
                    self.thread_power.moveToThread(self.thread2)
                    self.thread2.started.connect(self.thread_power.run)
                    self.thread_power.powerReturnError.connect(self.powerShowError)
                    self.thread_power.powerReturnEnd.connect(self.powerThreadEnd)
                    self.thread_power.powerReturnWarning.connect(self.powerShowWarning)
                    self.thread_power.powerReturnMaxPb.connect(self.setPowerMaxPb)
                    self.thread_power.powerReturnPb.connect(self.progressbar_power.setValue)
                    self.thread_power.powerReturnEmgLinkage.connect(self.setPowerEmgLinkage)
                    self.thread_power.powerReturnEmgMscode.connect(self.setPowerEmgMscode)
                    self.thread2.start()
                else:
                    self.enableRunBtn()
                    logging.info('착공지정일이 입력되지 않았습니다. 캘린더로부터 착공지정일을 선택해주세요.')
            else:
                logging.info('전원기종 착공량이 입력되지 않아 전원기종 착공은 미실시 됩니다.')
        else:
            self.enableRunBtn()
            logging.warning('필수 파일이 없어 더 이상 진행할 수 없습니다.')