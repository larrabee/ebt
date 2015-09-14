__author__ = 'larrabee'

import os
import sys
import re
import shutil
from datetime import datetime, timedelta


class Cleaner():
    def __init__(self, snap=None, dest=None, store_last=None, dayexp=None):
        self.__dayexp = dayexp
        self.__store_last = store_last
        self.__dest = dest
        self.__snap = snap
        self.__status = dict()

    @staticmethod
    def sort_by_date(string):
        return string[6:10] + string[3:5] + string[0:2] + string[11:13] + string[14:16]

    def __filter_list(self, path, format):
        """
        Format принимает текст и символы в формате Python regex, а так-же параметры в формате %parameter
        Список параметров:
        %date - заменяется на regex даты в формате "%d-%m-%Y_%H:%M" (обозначает текущую дату)
        %fdate - заменяется на regex даты в формате "%d-%m-%Y_%H:%M" (обозначает дату полного бэкапа)
        %% заменяется на % (используется как разделитель)
        """
        filtered_list = list()
        format = str(format).replace('%date', '([\d]{2}[-][\d]{2}[-][\d]{4}[_][\d]{2}[:][\d]{2})')
        format = str(format).replace('%fdate', '([\d]{2}[-][\d]{2}[-][\d]{4}[_][\d]{2}[:][\d]{2})')
        format = format.replace('%%', '[%%]')
        regex = '\A' + format + '\Z'
        comp_regex = re.compile(regex)
        dirs = os.listdir(path)
        for dir in dirs:
            if re.match(comp_regex, dir) is not None:
                filtered_list.append(dir)
        filtered_list.sort(reverse=True, key=self.sort_by_date)
        return filtered_list

    def __delete(self, lsdir, path, method=None):
        if self.__dayexp >= 0:
            dayexp_delta = timedelta(days=self.__dayexp)
            for backup in lsdir:
                if datetime.strptime(backup.split(sep='%')[0], '%d-%m-%Y_%H:%M') + dayexp_delta <= datetime.now():
                    try:
                        if method == 'file':
                            shutil.rmtree(path + '/' + backup)
                        elif method == 'btrfs':
                            Btrfs().sub_del(path + '/' + backup)
                        print("Remove {0}/{1}".format(path, backup))
                    except:
                        self.__status[backup] = sys.exc_info()
                    else:
                        self.__status[backup] = 'expired'
        if self.__store_last >= 0:
            for backup in lsdir[self.__store_last:]:
                try:
                    if method == 'file':
                        shutil.rmtree(path + '/' + backup)
                    elif method == 'btrfs':
                        Btrfs().sub_del(path + '/' + backup)
                    print("Remove {0}/{1}".format(path, backup))
                except:
                    self.__status[backup] = sys.exc_info()
                else:
                    self.__status[backup] = 'unwanted'

    def clean_snap(self):
        self.__delete(lsdir=self.__filter_list(format='%date', path=self.__snap), path=self.__snap, method='snap')

    def clean_file(self):
        self.__delete(lsdir=self.__filter_list(format='%date(%%%fdate)?', path=self.__dest), path=self.__dest)

    def clear_send(self):
        self.__delete(lsdir=self.__filter_list(format='%date(%%%fdate)?', path=self.__dest), path=self.__dest)
        self.__delete(lsdir=self.__filter_list(format='%date', path=self.__snap), path=self.__snap)

    def getresult(self):
        return self.__status
