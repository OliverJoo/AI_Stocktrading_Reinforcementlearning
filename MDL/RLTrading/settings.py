import os
import locale
import platform

LOGGER_NAME = 'RLTrading_v1'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if 'Linux' in platform.system() or 'Darwin' in platform.system():
    locale.setlocale(locale.LC_ALL, 'ko_KR.UTF-8')
elif 'Windows' in platform.system():
    locale.setlocale(locale.LC_ALL, '')
