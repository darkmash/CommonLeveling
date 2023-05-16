from PyQt5.QtCore import pyqtSignal, QObject
import time


class Worker(QObject):
    progressChanged = pyqtSignal(int)

    def work(self):
        progressbar_value = 0
        while progressbar_value < 100:
            self.progressChanged.emit(progressbar_value)
            time.sleep(0.1)