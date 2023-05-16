import datetime
import glob
import logging
from logging.handlers import RotatingFileHandler
import os
from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt, QCoreApplication
from PyQt5.QtGui import QStandardItemModel, QIcon, QIntValidator
from PyQt5.QtWidgets import QMainWindow, QProgressBar, QWidget, QGridLayout, QGroupBox, QLineEdit, QSizePolicy, QToolButton, QLabel, QFrame, QListView, QPushButton, QComboBox
from PyQt5.QtCore import pyqtSlot, QThread, QSize
import pandas as pd
from QPlainTextEditLogger import QPlainTextEditLogger
from CustomFormatter import CustomFormatter
from CalendarWindow import CalendarWindow
from UI_SubWindow import UI_SubWindow
from Fam3MainThread import Fam3MainThread
from Fam3PowerThread import Fam3PowerThread
from Fam3SpThread import Fam3SpThread
from JuxtaOldThread import JuxtaOldThread
from JuxtaCommonThread import JuxtaCommonThread


class Ui_SelectWindow(QMainWindow):
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
        self.lb_round = QLabel(self.groupBox)
        self.lb_round.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_round.setObjectName('lb_emgLinkageNoList')
        self.gridLayout3.addWidget(self.lb_round, 1, 0, 1, 1)
        self.le_mainOrderinput = QLineEdit(self.groupBox)
        self.le_mainOrderinput.setMinimumSize(QSize(0, 25))
        self.le_mainOrderinput.setObjectName('le_mainOrderinput')
        self.le_mainOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.le_mainOrderinput, 2, 1, 1, 1)
        self.le_spModuleOrderinput = QLineEdit(self.groupBox)
        self.le_spModuleOrderinput.setMinimumSize(QSize(0, 25))
        self.le_spModuleOrderinput.setObjectName('le_spModuleOrderinput')
        self.le_spModuleOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.le_spModuleOrderinput, 3, 1, 1, 1)
        self.le_spNonModuleOrderinput = QLineEdit(self.groupBox)
        self.le_spNonModuleOrderinput.setMinimumSize(QSize(0, 25))
        self.le_spNonModuleOrderinput.setObjectName('le_spModuleOrderinput')
        self.le_spNonModuleOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.le_spNonModuleOrderinput, 4, 1, 1, 1)
        self.le_powerOrderinput = QLineEdit(self.groupBox)
        self.le_powerOrderinput.setMinimumSize(QSize(0, 25))
        self.le_powerOrderinput.setObjectName('le_powerOrderinput')
        self.le_powerOrderinput.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.le_powerOrderinput, 5, 1, 1, 1)
        self.le_oldSecOrd = QLineEdit(self.groupBox)
        self.le_oldSecOrd.setMinimumSize(QSize(0, 25))
        self.le_oldSecOrd.setObjectName('le_oldFirstOrd')
        self.le_oldSecOrd.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.le_oldSecOrd, 3, 2, 1, 1)
        self.le_commonSecOrd = QLineEdit(self.groupBox)
        self.le_commonSecOrd.setMinimumSize(QSize(0, 25))
        self.le_commonSecOrd.setObjectName('le_commonSecOrd')
        self.le_commonSecOrd.setValidator(QIntValidator(self))
        self.gridLayout3.addWidget(self.le_commonSecOrd, 4, 2, 1, 1)
        self.dateBtn = QToolButton(self.groupBox)
        self.dateBtn.setMinimumSize(QSize(0, 25))
        self.dateBtn.setObjectName('dateBtn')
        self.gridLayout3.addWidget(self.dateBtn, 6, 1, 1, 1)
        self.emgFileInputBtn = QPushButton(self.groupBox)
        self.emgFileInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.emgFileInputBtn, 7, 1, 1, 1)
        self.holdFileInputBtn = QPushButton(self.groupBox)
        self.holdFileInputBtn.setMinimumSize(QSize(0, 25))
        self.gridLayout3.addWidget(self.holdFileInputBtn, 10, 1, 1, 1)
        self.lb_1stOrder = QLabel(self.groupBox)
        self.lb_1stOrder.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_1stOrder.setObjectName('lb_1stOrder')
        self.lb_2ndOrder = QLabel(self.groupBox)
        self.lb_2ndOrder.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_2ndOrder.setObjectName('lb_2ndOrder')
        self.lb_emgLinkageNoList = QLabel(self.groupBox)
        self.lb_emgLinkageNoList.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_emgLinkageNoList.setObjectName('lb_emgLinkageNoList')
        self.gridLayout3.addWidget(self.lb_emgLinkageNoList, 8, 1, 1, 1)
        self.lb_emgMsCodeList = QLabel(self.groupBox)
        self.lb_emgMsCodeList.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_emgMsCodeList.setObjectName('lb_emgMsCodeList')
        self.gridLayout3.addWidget(self.lb_emgMsCodeList, 8, 2, 1, 1)
        self.lb_holdLinkageNoList = QLabel(self.groupBox)
        self.lb_holdLinkageNoList.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_holdLinkageNoList.setObjectName('lb_holdLinkageNoList')
        self.gridLayout3.addWidget(self.lb_holdLinkageNoList, 11, 1, 1, 1)
        self.lb_holdMsCodeList = QLabel(self.groupBox)
        self.lb_holdMsCodeList.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_holdMsCodeList.setObjectName('lb_holdMsCodeList')
        self.gridLayout3.addWidget(self.lb_holdMsCodeList, 11, 2, 1, 1)
        listViewModelEmgLinkage = QStandardItemModel()
        self.listViewEmgLinkage = QListView(self.groupBox)
        self.listViewEmgLinkage.setModel(listViewModelEmgLinkage)
        self.gridLayout3.addWidget(self.listViewEmgLinkage, 9, 1, 1, 1)
        listViewModelEmgmscode = QStandardItemModel()
        self.listViewEmgmscode = QListView(self.groupBox)
        self.listViewEmgmscode.setModel(listViewModelEmgmscode)
        self.gridLayout3.addWidget(self.listViewEmgmscode, 9, 2, 1, 1)
        listViewModelHoldLinkage = QStandardItemModel()
        self.listViewHoldLinkage = QListView(self.groupBox)
        self.listViewHoldLinkage.setModel(listViewModelHoldLinkage)
        self.gridLayout3.addWidget(self.listViewHoldLinkage, 12, 1, 1, 1)
        listViewModelHoldmscode = QStandardItemModel()
        self.listViewHoldmscode = QListView(self.groupBox)
        self.listViewHoldmscode.setModel(listViewModelHoldmscode)
        self.gridLayout3.addWidget(self.listViewHoldmscode, 12, 2, 1, 1)
        self.lb_blank = QLabel(self.groupBox)
        self.lb_blank.setObjectName('lb_blank')
        self.gridLayout3.addWidget(self.lb_blank, 5, 4, 1, 1)
        self.progressbar_main = QProgressBar(self.groupBox)
        self.progressbar_main.setObjectName('progressbar_main')
        self.progressbar_main.setAlignment(Qt.AlignVCenter)
        self.progressbar_main.setFormat('FAM3 메인라인 진행률')
        self.gridLayout3.addWidget(self.progressbar_main, 13, 1, 1, 2)
        self.progressbar_sp = QProgressBar(self.groupBox)
        self.progressbar_sp.setObjectName('progressbar_sp')
        self.progressbar_sp.setAlignment(Qt.AlignVCenter)
        self.progressbar_sp.setFormat('FAM3 특수라인 진행률')
        self.gridLayout3.addWidget(self.progressbar_sp, 14, 1, 1, 2)
        self.progressbar_power = QProgressBar(self.groupBox)
        self.progressbar_power.setObjectName('progressbar_power')
        self.progressbar_power.setAlignment(Qt.AlignVCenter)
        self.progressbar_power.setFormat('FAM3 전원라인 진행률')
        self.gridLayout3.addWidget(self.progressbar_power, 15, 1, 1, 2)
        self.runBtn = QToolButton(self.groupBox)
        sizePolicy = QSizePolicy(QSizePolicy.Ignored, QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.runBtn.sizePolicy().hasHeightForWidth())
        self.runBtn.setSizePolicy(sizePolicy)
        self.runBtn.setMinimumSize(QSize(30, 35))
        self.runBtn.setStyleSheet('background-color: rgb(63, 63, 63);\ncolor: rgb(255, 255, 255);')
        self.runBtn.setObjectName('runBtn')
        self.gridLayout3.addWidget(self.runBtn, 17, 3, 1, 2)
        self.lb_line = QLabel(self.groupBox)
        self.lb_line.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_line.setObjectName('lb_line')
        self.gridLayout3.addWidget(self.lb_line, 0, 0, 1, 1)
        self.label = QLabel(self.groupBox)
        self.label.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.label.setObjectName('label')
        self.gridLayout3.addWidget(self.label, 2, 0, 1, 1)
        self.lb_spModule = QLabel(self.groupBox)
        self.lb_spModule.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_spModule.setObjectName('lb_spModule')
        self.gridLayout3.addWidget(self.lb_spModule, 3, 0, 1, 1)
        self.lb_power = QLabel(self.groupBox)
        self.lb_power.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_power.setObjectName('lb_power')
        self.gridLayout3.addWidget(self.lb_power, 5, 0, 1, 1)
        self.lb_spNonModule = QLabel(self.groupBox)
        self.lb_spNonModule.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_spNonModule.setObjectName('lb_spNonModule')
        self.gridLayout3.addWidget(self.lb_spNonModule, 4, 0, 1, 1)
        self.lb_inputDate = QLabel(self.groupBox)
        self.lb_inputDate.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_inputDate.setObjectName('lb_inputDate')
        self.gridLayout3.addWidget(self.lb_inputDate, 6, 0, 1, 1)
        self.lb_date = QLabel(self.groupBox)
        self.lb_date.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_date.setObjectName('lb_date')
        self.gridLayout3.addWidget(self.lb_date, 6, 2, 1, 1)
        self.lb_emgInput = QLabel(self.groupBox)
        self.lb_emgInput.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_emgInput.setObjectName('lb_emgInput')
        self.gridLayout3.addWidget(self.lb_emgInput, 7, 0, 1, 1)
        self.lb_holdInput = QLabel(self.groupBox)
        self.lb_holdInput.setAlignment(Qt.AlignRight | Qt.AlignTrailing | Qt.AlignVCenter)
        self.lb_holdInput.setObjectName('lb_holdInput')
        self.gridLayout3.addWidget(self.lb_holdInput, 10, 0, 1, 1)
        self.line = QFrame(self.groupBox)
        self.line.setFrameShape(QFrame.HLine)
        self.line.setFrameShadow(QFrame.Sunken)
        self.line.setObjectName('line')
        self.cb_round = QComboBox(self.groupBox)
        self.gridLayout3.addWidget(self.cb_round, 1, 1, 1, 1)
        self.cb_line = QComboBox(self.groupBox)
        self.gridLayout3.addWidget(self.cb_line, 0, 1, 1, 1)
        self.gridLayout3.addWidget(self.line, 16, 0, 1, 10)
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
        self.dateBtn.clicked.connect(self.selectStartDate)
        self.emgFileInputBtn.clicked.connect(self.emgWindow)
        self.holdFileInputBtn.clicked.connect(self.holdWindow)
        self.runBtn.clicked.connect(self.startLeveling)
        # 디버그용 플래그
        self.isDebug = True
        self.isFileReady = False
        self.MaxOrderInputFilePath = r'.\\input\\FAM3\\1차_착공량입력.xlsx'
        self.etcOrderInputFilePath = r'.\\input\\FAM3\\2차_착공량입력.xlsx'
        self.df_etcOrderInput = None
        self.cb_round.currentTextChanged.connect(self.readMaxOrderFile)
        self.cb_line.activated[str].connect(self.changeUtaUi)
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
            self.le_debugDate = QLineEdit(self.groupBox)
            self.le_debugDate.setObjectName('date')
            self.gridLayout3.addWidget(self.le_debugDate, 13, 0, 1, 1)
            self.le_debugDate.setPlaceholderText('디버그용 날짜입력')
        self.retranslateUi(self)
        self.thread = QThread()
        self.thread.setTerminationEnabled(True)
        self.thread2 = QThread()
        self.thread2.setTerminationEnabled(True)
        self.thread3 = QThread()
        self.thread3.setTerminationEnabled(True)
        self.show()

    def retranslateUi(self, MainWindow):
        _translate = QCoreApplication.translate
        MainWindow.setWindowTitle(_translate('MainWindow', '착공 평준화 자동화 프로그램 Rev0.01'))
        MainWindow.setWindowIcon(QIcon('.\\Logo\\logo.png'))
        self.lb_line.setText(_translate('MainWindow', '라인 선택:'))
        self.lb_round.setText(_translate('MainWindow', '착공 회차 선택:'))
        self.label.setText(_translate('MainWindow', '메인 생산대수:'))
        self.lb_spModule.setText(_translate('MainWindow', '특수(모듈) 생산대수:'))
        self.lb_spNonModule.setText(_translate('MainWindow', '특수(비모듈) 생산대수:'))
        self.lb_power.setText(_translate('MainWindow', '전원 생산대수:'))
        self.runBtn.setText(_translate('MainWindow', '실행'))
        self.lb_emgInput.setText(_translate('MainWindow', '긴급오더 입력 :'))
        self.lb_holdInput.setText(_translate('MainWindow', '홀딩오더 입력 :'))
        self.lb_emgLinkageNoList.setText(_translate('MainWindow', 'Linkage No List'))
        self.lb_emgMsCodeList.setText(_translate('MainWindow', 'MSCode List'))
        self.lb_holdLinkageNoList.setText(_translate('MainWindow', 'Linkage No List'))
        self.lb_holdMsCodeList.setText(_translate('MainWindow', 'MSCode List'))
        self.lb_inputDate.setText(_translate('MainWndow', '착공지정일 입력 :'))
        self.lb_date.setText(_translate('MainWndow', '미선택'))
        self.dateBtn.setText(_translate('MainWindow', ' 착공지정일 선택 '))
        self.emgFileInputBtn.setText(_translate('MainWindow', '리스트 입력'))
        self.holdFileInputBtn.setText(_translate('MainWindow', '리스트 입력'))
        self.lb_blank.setText(_translate('MainWindow', '            '))
        list_round = ['1차', '2차']
        list_line = ['UTA', 'FAM3', 'JUXTA']
        self.cb_round.addItems(list_round)
        self.cb_line.addItems(list_line)
        self.changeUtaUi('UTA')
        logging.info('프로그램이 정상 기동했습니다')

    def changeUtaUi(self, line):
        _translate = QCoreApplication.translate
        if line == 'UTA':
            self.lb_round.hide()
            self.cb_round.hide()
            self.lb_spModule.hide()
            self.lb_spNonModule.hide()
            self.lb_power.hide()
            self.lb_1stOrder.hide()
            self.lb_2ndOrder.hide()
            self.progressbar_power.hide()
            self.progressbar_sp.hide()
            self.le_spModuleOrderinput.hide()
            self.le_spNonModuleOrderinput.hide()
            self.le_powerOrderinput.hide()
            self.le_commonSecOrd.hide()
            self.le_oldSecOrd.hide()
            self.label.setText(_translate('MainWindow', '총 생산대수 :'))
            self.lb_date.setText(_translate('MainWndow', '미선택'))
            self.gridLayout3.addWidget(self.label, 1, 0, 1, 1)
            self.gridLayout3.addWidget(self.le_mainOrderinput, 1, 1, 1, 1)
            self.gridLayout3.addWidget(self.dateBtn, 2, 1, 1, 1)
            self.gridLayout3.addWidget(self.emgFileInputBtn, 3, 1, 1, 1)
            self.gridLayout3.addWidget(self.holdFileInputBtn, 6, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_emgLinkageNoList, 4, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_emgMsCodeList, 4, 2, 1, 1)
            self.gridLayout3.addWidget(self.lb_holdLinkageNoList, 7, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_holdMsCodeList, 7, 2, 1, 1)
            self.gridLayout3.addWidget(self.listViewEmgLinkage, 5, 1, 1, 1)
            self.gridLayout3.addWidget(self.listViewEmgmscode, 5, 2, 1, 1)
            self.gridLayout3.addWidget(self.listViewHoldLinkage, 8, 1, 1, 1)
            self.gridLayout3.addWidget(self.listViewHoldmscode, 8, 2, 1, 1)
            self.gridLayout3.addWidget(self.lb_blank, 2, 4, 1, 1)
            self.gridLayout3.addWidget(self.progressbar_main, 11, 1, 1, 2)
            self.progressbar_main.setFormat('UTA 진행률')
            self.gridLayout3.addWidget(self.runBtn, 11, 3, 1, 2)
            self.gridLayout3.addWidget(self.lb_inputDate, 2, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_date, 2, 2, 1, 1)
            self.gridLayout3.addWidget(self.lb_emgInput, 3, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_holdInput, 6, 0, 1, 1)
            if self.isDebug:
                self.gridLayout3.addWidget(self.le_debugDate, 11, 0, 1, 1)
        elif line == 'FAM3':
            self.lb_round.show()
            self.cb_round.show()
            self.lb_spModule.show()
            self.lb_spNonModule.show()
            self.lb_power.show()
            self.lb_1stOrder.hide()
            self.lb_2ndOrder.hide()
            self.progressbar_power.show()
            self.progressbar_sp.show()
            self.le_spModuleOrderinput.show()
            self.le_spNonModuleOrderinput.show()
            self.le_powerOrderinput.show()
            self.le_commonSecOrd.hide()
            self.le_oldSecOrd.hide()
            self.gridLayout3.addWidget(self.lb_round, 1, 0, 1, 1)
            self.gridLayout3.addWidget(self.cb_round, 1, 1, 1, 1)
            self.gridLayout3.addWidget(self.le_mainOrderinput, 2, 1, 1, 1)
            self.gridLayout3.addWidget(self.le_spModuleOrderinput, 3, 1, 1, 1)
            self.gridLayout3.addWidget(self.le_spNonModuleOrderinput, 4, 1, 1, 1)
            self.gridLayout3.addWidget(self.le_powerOrderinput, 5, 1, 1, 1)
            self.gridLayout3.addWidget(self.dateBtn, 6, 1, 1, 1)
            self.gridLayout3.addWidget(self.emgFileInputBtn, 7, 1, 1, 1)
            self.gridLayout3.addWidget(self.holdFileInputBtn, 10, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_emgLinkageNoList, 8, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_emgMsCodeList, 8, 2, 1, 1)
            self.gridLayout3.addWidget(self.lb_holdLinkageNoList, 11, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_holdMsCodeList, 11, 2, 1, 1)
            self.gridLayout3.addWidget(self.listViewEmgLinkage, 9, 1, 1, 1)
            self.gridLayout3.addWidget(self.listViewEmgmscode, 9, 2, 1, 1)
            self.gridLayout3.addWidget(self.listViewHoldLinkage, 12, 1, 1, 1)
            self.gridLayout3.addWidget(self.listViewHoldmscode, 12, 2, 1, 1)
            self.gridLayout3.addWidget(self.lb_blank, 5, 4, 1, 1)
            self.gridLayout3.addWidget(self.progressbar_main, 13, 1, 1, 2)
            self.progressbar_main.setFormat('FAM3 메인라인 진행률')
            self.gridLayout3.addWidget(self.progressbar_sp, 14, 1, 1, 2)
            self.progressbar_sp.setFormat('FAM3 특수라인 진행률')
            self.gridLayout3.addWidget(self.progressbar_power, 15, 1, 1, 2)
            self.progressbar_power.setFormat('FAM3 전원라인 진행률')
            self.gridLayout3.addWidget(self.runBtn, 17, 3, 1, 2)
            self.gridLayout3.addWidget(self.lb_line, 0, 0, 1, 1)
            self.gridLayout3.addWidget(self.label, 2, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_spModule, 3, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_power, 5, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_spNonModule, 4, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_inputDate, 6, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_date, 6, 2, 1, 1)
            self.gridLayout3.addWidget(self.lb_emgInput, 7, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_holdInput, 10, 0, 1, 1)
            self.lb_line.setText(_translate('MainWindow', '라인 선택:'))
            self.lb_round.setText(_translate('MainWindow', '착공 회차 선택:'))
            self.label.setText(_translate('MainWindow', '메인 생산대수:'))
            self.lb_spModule.setText(_translate('MainWindow', '특수(모듈) 생산대수:'))
            self.lb_spNonModule.setText(_translate('MainWindow', '특수(비모듈) 생산대수:'))
            self.runBtn.setText(_translate('MainWindow', '실행'))
            self.lb_date.setText(_translate('MainWndow', '미선택'))
            if self.isDebug:
                self.gridLayout3.addWidget(self.le_debugDate, 13, 0, 1, 1)
            self.df_etcOrderInput = self.readMaxOrderFile(line)
        elif line == 'JUXTA':
            self.label.hide()
            self.lb_round.hide()
            self.cb_round.hide()
            self.le_commonSecOrd.show()
            self.le_oldSecOrd.show()
            self.le_spModuleOrderinput.show()
            self.le_spNonModuleOrderinput.hide()
            self.le_powerOrderinput.hide()
            self.lb_spModule.show()
            self.lb_power.hide()
            self.lb_spNonModule.show()
            self.lb_1stOrder.show()
            self.lb_2ndOrder.show()
            self.progressbar_power.hide()
            self.progressbar_sp.show()
            self.gridLayout3.addWidget(self.lb_1stOrder, 2, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_2ndOrder, 2, 2, 1, 1)
            self.gridLayout3.addWidget(self.lb_spModule, 3, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_spNonModule, 4, 0, 1, 1)
            self.gridLayout3.addWidget(self.le_mainOrderinput, 3, 1, 1, 1)
            self.gridLayout3.addWidget(self.le_spModuleOrderinput, 4, 1, 1, 1)
            self.gridLayout3.addWidget(self.le_oldSecOrd, 3, 2, 1, 1)
            self.gridLayout3.addWidget(self.le_commonSecOrd, 4, 2, 1, 1)
            self.gridLayout3.addWidget(self.dateBtn, 5, 1, 1, 1)
            self.gridLayout3.addWidget(self.emgFileInputBtn, 6, 1, 1, 1)
            self.gridLayout3.addWidget(self.holdFileInputBtn, 10, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_emgLinkageNoList, 8, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_emgMsCodeList, 8, 2, 1, 1)
            self.gridLayout3.addWidget(self.lb_holdLinkageNoList, 11, 1, 1, 1)
            self.gridLayout3.addWidget(self.lb_holdMsCodeList, 11, 2, 1, 1)
            self.gridLayout3.addWidget(self.listViewEmgLinkage, 9, 1, 1, 1)
            self.gridLayout3.addWidget(self.listViewEmgmscode, 9, 2, 1, 1)
            self.gridLayout3.addWidget(self.listViewHoldLinkage, 12, 1, 1, 1)
            self.gridLayout3.addWidget(self.listViewHoldmscode, 12, 2, 1, 1)
            self.gridLayout3.addWidget(self.lb_blank, 5, 4, 1, 1)
            self.gridLayout3.addWidget(self.progressbar_main, 13, 1, 1, 2)
            self.progressbar_main.setFormat('JUXTA 현행라인 진행률')
            self.gridLayout3.addWidget(self.progressbar_sp, 14, 1, 1, 2)
            self.progressbar_sp.setFormat('JUXTA 공통화라인 진행률')
            self.gridLayout3.addWidget(self.runBtn, 17, 3, 1, 2)
            self.gridLayout3.addWidget(self.lb_line, 0, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_inputDate, 5, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_date, 5, 2, 2, 2)
            self.gridLayout3.addWidget(self.lb_emgInput, 6, 0, 1, 1)
            self.gridLayout3.addWidget(self.lb_holdInput, 10, 0, 1, 1)
            if self.isDebug:
                self.gridLayout3.addWidget(self.le_debugDate, 13, 0, 1, 1)
            self.label.setText(_translate('MainWindow', ''))
            self.lb_1stOrder.setText(_translate('MainWindow', '1차 착공'))
            self.lb_2ndOrder.setText(_translate('MainWindow', '2차 착공'))
            self.lb_spModule.setText(_translate('MainWindow', 'JUXTA현행 생산대수:'))
            self.lb_spNonModule.setText(_translate('MainWindow', 'JUXTA공통화 생산대수:'))
            self.df_etcOrderInput = self.readMaxOrderFile(line)

    # 최대착공량입력 파일 불러오기 함수
    def readMaxOrderFile(self, line):
        if line == 'FAM3':
            if self.cb_round.currentText() == "1차":
                self.MaxOrderInputFilePath = r'.\\input\\1차_착공량입력.xlsx'
                self.etcOrderInputFilePath = r'.\\input\\2차_착공량입력.xlsx'
            elif self.cb_round.currentText() == "2차":
                self.MaxOrderInputFilePath = r'.\\input\\2차_착공량입력.xlsx'
                self.etcOrderInputFilePath = r'.\\input\\1차_착공량입력.xlsx'
            if os.path.exists(self.MaxOrderInputFilePath):
                df_orderInput = pd.read_excel(self.MaxOrderInputFilePath)
                self.le_mainOrderinput.setText(str(df_orderInput['착공량'][0]))
                self.le_spModuleOrderinput.setText(str(df_orderInput['착공량'][1]))
                self.le_spNonModuleOrderinput.setText(str(df_orderInput['착공량'][2]))
                self.le_powerOrderinput.setText(str(df_orderInput['착공량'][3]))
            else:
                logging.error('%s 파일이 없습니다. 착공량을 수동으로 입력해주세요.', self.MaxOrderInputFilePath)
            return df_orderInput
        elif line == 'JUXTA':
            self.MaxOrderInputFilePath = r'.\\input\\1차_착공량입력.xlsx'
            self.etcOrderInputFilePath = r'.\\input\\2차_착공량입력.xlsx'
            if os.path.exists(self.MaxOrderInputFilePath):
                df_1stOrderInput = pd.read_excel(self.MaxOrderInputFilePath)
                self.le_mainOrderinput.setText(str(df_1stOrderInput['착공량'][4]))
                self.le_spModuleOrderinput.setText(str(df_1stOrderInput['착공량'][5]))
            else:
                logging.error('%s 파일이 없습니다. 1차 착공량을 수동으로 입력해주세요.', self.MaxOrderInputFilePath)
            if os.path.exists(self.etcOrderInputFilePath):
                df_2ndOrderInput = pd.read_excel(self.etcOrderInputFilePath)
                self.le_oldSecOrd.setText(str(df_2ndOrderInput['착공량'][4]))
                self.le_commonSecOrd.setText(str(df_2ndOrderInput['착공량'][5]))
            else:
                logging.error('%s 파일이 없습니다. 2차 착공량을 수동으로 입력해주세요.', self.etcOrderInputFilePath)
            return None

    # 착공지정일 캘린더 호출
    def selectStartDate(self):
        self.calendar_window = CalendarWindow()
        self.calendar_window.submitClicked.connect(self.getDate)

    def getDate(self, date):
        if len(date) > 0:
            self.lb_date.setText(date)
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
            self.lb_date.setText(date)
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
    def fam3MainEnd(self, isEnd):
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
    def fam3PowerEnd(self, isEnd):
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
    def fam3SpEnd(self, isEnd):
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

    # Juxta현행라인 쓰레드 종료용 함수
    def juxtaOldEnd(self, isEnd):
        if isEnd:
            logging.info('Juxta 현행라인 착공이 완료되었습니다.')
            self.thread.quit()
            self.thread.wait()
            self.enableRunBtn()

    # Juxta공통화라인 쓰레드 종료용 함수
    def juxtaCommonEnd(self, isEnd):
        if isEnd:
            logging.info('Juxta 공통화라인 착공이 완료되었습니다.')
            self.thread2.quit()
            self.thread2.wait()
            self.enableRunBtn()

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

    # Fam3 메인라인 프로그레스바 범위 설정용 함수
    def setFam3MainMaxPb(self, maxPb):
        self.progressbar_main.setRange(0, maxPb)

    # Fam3 전원라인 프로그레스바 범위 설정용 함수
    def setFam3PowerMaxPb(self, maxPb):
        self.progressbar_power.setRange(0, maxPb)

    # Fam3 특수라인 프로그레스바 범위 설정용 함수
    def setFam3SpMaxPb(self, maxPb):
        self.progressbar_sp.setRange(0, maxPb)
    
    # Juxta 현행 프로그레스바 범위 설정용 함수
    def setJuxtaOldMaxPb(self, maxPb):
        self.progressbar_main.setRange(0, maxPb)

    # Juxta 공통화 프로그레스바 범위 설정용 함수
    def setJuxtaCommonMaxPb(self, maxPb):
        self.progressbar_sp.setRange(0, maxPb)

    # FAM3 마스터파일 불러오기 함수
    def loadFam3MstFile(self):
        self.isFileReady = True
        masterFileList = []
        date = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            date = self.le_debugDate.text()
        roundTxt = self.cb_round.currentText()
        sosFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\SOS2.xlsx'
        if float(self.le_mainOrderinput.text()) != 0.0:
            mainFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\MAIN.xlsx'
        else:
            mainFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.le_spModuleOrderinput.text()) != 0.0:
            spFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\OTHER.xlsx'
        else:
            spFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.le_powerOrderinput.text()) != 0.0:
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
        if float(self.le_spNonModuleOrderinput.text()) != 0.0:
            if os.path.exists(r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\BL.xlsx'):
                nonSpBLFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\BL.xlsx'
            else:
                nonSpBLFilePath = r'.\\input\\FAM3\\Master_File\\' + date + r'\\'
        else:
            nonSpBLFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.le_spNonModuleOrderinput.text()) != 0.0:
            if os.path.exists(r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\TERMINAL.xlsx'):
                nonSpTerminalFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\TERMINAL.xlsx'
            else:
                nonSpTerminalFilePath = r'.\\input\\FAM3\\Master_File\\' + date + r'\\'
        else:
            nonSpTerminalFilePath = r'.\\input\\FAM3\\Master_File\\' + date + '\\' + roundTxt + r'\\'
        if float(self.le_spNonModuleOrderinput.text()) != 0.0:
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

    # 마스터파일 불러오기 함수
    def loadJuxtaMstFile(self):
        self.isFileReady = True
        masterFileList = []
        date = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            date = self.le_debugDate.text()
        sosFilePath = r'.\\input\\JUXTA\\Master_File\\' + date + '\\' + r'\\SOS2.xlsx'
        oldLevelingFilePath = r'.\\input\\JUXTA\\Master_File\\' + date + '\\' + r'\\5400_A0100A12_' + date +'_Leveling_List.xlsx'   
        commonLevelingFilePath = r'.\\input\\JUXTA\\Master_File\\' + date + '\\' + r'\\5400_A0100A17_' + date +'_Leveling_List.xlsx'   
        oldCondFilePath = r'.\\input\\JUXTA\\DB\\JUXTA_착공_기준표(현행).xlsx'
        commonCondFilePath = r'.\\input\\JUXTA\\DB\\JUXTA_착공_기준표(공통화).xlsx'
        configFilePath = r'.\\Config.ini'
        holdingListFilePath = r'.\\input\\JUXTA\\홀딩리스트 및 기출력Tag.xlsx'
        # 위의 마스터파일 경로들을 리스트화
        pathList = [sosFilePath,
                    oldLevelingFilePath,
                    commonLevelingFilePath,
                    oldCondFilePath,
                    commonCondFilePath,
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

    # 착공시작 함수
    @pyqtSlot()
    def startLeveling(self):
        self.disableRunBtn()
        self.progressbar_sp.setValue(0)
        date = datetime.datetime.today().strftime('%Y%m%d')
        if self.isDebug:
            date = self.le_debugDate.text()
        list_emgHold = self.loadEmgHoldList()
        if self.cb_line.currentText() == 'FAM3':
            list_masterFile = self.loadFam3MstFile()
            if len(self.le_mainOrderinput.text()) > 0:
                if self.lb_date.text() != '미선택':
                    self.thread_main = Fam3MainThread(self.isDebug,
                                                        date,
                                                        self.lb_date.text(),
                                                        list_masterFile,
                                                        float(self.le_mainOrderinput.text()),
                                                        list_emgHold,
                                                        self.cb_round.currentText(),
                                                        self.df_etcOrderInput)
                    self.thread_main.moveToThread(self.thread)
                    self.thread.started.connect(self.thread_main.run)
                    self.thread_main.mainReturnError.connect(self.mainShowError)
                    self.thread_main.mainReturnEnd.connect(self.fam3MainEnd)
                    self.thread_main.mainReturnWarning.connect(self.mainShowWarning)
                    self.thread_main.mainReturnDf.connect(self.startSpLeveling)
                    self.thread_main.mainReturnMaxPb.connect(self.setFam3MainMaxPb)
                    self.thread_main.mainReturnPb.connect(self.progressbar_main.setValue)
                    self.thread_main.mainReturnEmgLinkage.connect(self.setMainEmgLinkage)
                    self.thread_main.mainReturnEmgMscode.connect(self.setMainEmgMscode)
                    self.thread.start()
                else:
                    self.enableRunBtn()
                    logging.info('착공지정일이 입력되지 않았습니다. 캘린더로부터 착공지정일을 선택해주세요.')
            else:
                logging.info('메인기종 착공량이 입력되지 않아 메인기종 착공은 미실시 됩니다.')
            if len(self.le_powerOrderinput.text()) > 0:
                if self.lb_date.text() != '미선택':
                    self.thread_power = Fam3PowerThread(self.isDebug,
                                                        date,
                                                        self.lb_date.text(),
                                                        list_masterFile,
                                                        float(self.le_powerOrderinput.text()),
                                                        list_emgHold,
                                                        self.cb_round.currentText(),
                                                        self.df_etcOrderInput)
                    self.thread_power.moveToThread(self.thread2)
                    self.thread2.started.connect(self.thread_power.run)
                    self.thread_power.powerReturnError.connect(self.powerShowError)
                    self.thread_power.powerReturnEnd.connect(self.fam3PowerEnd)
                    self.thread_power.powerReturnWarning.connect(self.powerShowWarning)
                    self.thread_power.powerReturnMaxPb.connect(self.setFam3PowerMaxPb)
                    self.thread_power.powerReturnPb.connect(self.progressbar_power.setValue)
                    self.thread_power.powerReturnEmgLinkage.connect(self.setPowerEmgLinkage)
                    self.thread_power.powerReturnEmgMscode.connect(self.setPowerEmgMscode)
                    self.thread2.start()
                else:
                    self.enableRunBtn()
                    logging.info('착공지정일이 입력되지 않았습니다. 캘린더로부터 착공지정일을 선택해주세요.')
            else:
                logging.info('전원기종 착공량이 입력되지 않아 전원기종 착공은 미실시 됩니다.')
            if len(self.le_spModuleOrderinput.text()) > 0:
                if self.lb_date.text() != '미선택':
                    self.thread_sp = Fam3SpThread(self.isDebug,
                                                    date,
                                                    self.lb_date.text(),
                                                    list_masterFile,
                                                    float(self.le_spModuleOrderinput.text()),
                                                    float(self.le_spNonModuleOrderinput.text()),
                                                    list_emgHold,
                                                    self.cb_round.currentText(),
                                                    self.df_etcOrderInput)
                    self.thread_sp.moveToThread(self.thread3)
                    self.thread3.started.connect(self.thread_sp.run)
                    self.thread_sp.spReturnError.connect(self.spShowError)
                    self.thread_sp.spReturnEnd.connect(self.fam3SpEnd)
                    self.thread_sp.spReturnWarning.connect(self.spShowWarning)
                    self.thread_sp.spReturnMaxPb.connect(self.setFam3SpMaxPb)
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
        elif self.cb_line.currentText() == 'JUXTA':
            list_masterFile = self.loadJuxtaMstFile()
            if len(self.le_mainOrderinput.text()) > 0 and len(self.le_oldSecOrd.text()) > 0:
                if self.lb_date.text() != '미선택':
                    self.thread_juxtaOld = JuxtaOldThread(self.isDebug,
                                                            date,
                                                            self.lb_date.text(),
                                                            list_masterFile,
                                                            float(self.le_mainOrderinput.text()),
                                                            float(self.le_oldSecOrd.text()),
                                                            list_emgHold)
                    self.thread_juxtaOld.moveToThread(self.thread)
                    self.thread_juxtaOld.returnEnd.connect(self.juxtaOldEnd)
                    self.thread_juxtaOld.returnMaxPb.connect(self.setJuxtaOldMaxPb)
                    self.thread_juxtaOld.returnPb.connect(self.progressbar_main.setValue)
                    self.thread.started.connect(self.thread_juxtaOld.run)
                    
                    self.thread.start()
            
if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ui = Ui_SelectWindow()
    sys.exit(app.exec_())
