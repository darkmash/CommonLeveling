import logging
from PyQt5 import QtGui


class CustomFormatter(logging.Formatter):
    FORMATS = {
        logging.ERROR: ('[%(asctime)s] %(levelname)s:%(message)s', 'yellow'),
        logging.DEBUG: ('[%(asctime)s] %(levelname)s:%(message)s', 'white'),
        logging.INFO: ('[%(asctime)s] %(levelname)s:%(message)s', 'white'),
        logging.WARNING: ('[%(asctime)s] %(levelname)s:%(message)s', 'yellow')
    }

    def format(self, record):
        last_fmt = self._style._fmt
        opt = CustomFormatter.FORMATS.get(record.levelno)
        if opt:
            fmt, color = opt
            self._style._fmt = "<font color=\"{}\">{}</font>".format(QtGui.QColor(color).name(), fmt)
        res = logging.Formatter.format(self, record)
        self._style._fmt = last_fmt
        return res
