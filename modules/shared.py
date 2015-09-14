__author__ = 'larrabee'
import os
import re
import subprocess
import zlib
import hashlib
from datetime import datetime



class LvmBadVGPath(Exception):
    def __init__(self, *args):
        self.args = args

    def __str__(self):
        return self.args


class LvmCreateSnapshotError(Exception):
    def __init__(self, *args):
        self.args = args

    def __str__(self):
        return self.args


class LvmRemoveSnapshotError(Exception):
    def __init__(self, *args):
        self.args = args

    def __str__(self):
        return self.args


class DDWrongMethod(Exception):
    def __init__(self, *args):
        self.args = args

    def __str__(self):
        return self.args


class LvmSnapManager():
    def __init__(self, vg):
        self.vg = vg
        self.vg = self.__check_vg()

    def __check_vg(self):
        if (re.match(r'\A(/[\w\d\-\.:\[\]\\,]+)+/?\Z', self.vg) is None) or (not os.path.exists(self.vg)):
            raise LvmBadVGPath('VG: ', self.vg)
        if self.vg[-1:] == '/':
            return self.vg[:-1]
        else:
            return self.vg

    def create_snap(self, lv, snapshot_size='10G'):
        snapshot_name = lv + '%' + str(datetime.now().strftime(format='%Y-%m-%d_%H:%M:%S'))
        command = {'lvcreate', '-s', '-L{0}'.format(snapshot_size), '-n', snapshot_name, self.vg}
        process = subprocess.Popen(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        output = str(process.communicate())
        if process.returncode != 0:
            raise LvmCreateSnapshotError('VG: ', self.vg, 'LV: ', lv, 'Error code: ',
                                         str(process.returncode), 'Output: ', output)
        return snapshot_name

    def remove_snap(self, snapshot_name):
        command = {'lvremove', '-n', snapshot_name, self.vg}
        process = subprocess.Popen(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        output = str(process.communicate())
        if process.returncode != 0:
            raise LvmRemoveSnapshotError('VG: ', self.vg, 'LV: ', snapshot_name, 'Error code: ',
                                         str(process.returncode), 'Output: ', output)
        return

    def list_vg(self):
        list = os.listdir(self.vg)
        return list

    def cleanup(self):
        lvs = self.list_vg()
        removed_snap = list
        not_removed_snap = list
        for lv in lvs:
            if re.match(r'\A([\w\d\-]+)+([%%])([\d\-:_])+\Z', lv) is not None:
                try:
                    self.remove_snap(lv)
                except LvmRemoveSnapshotError:
                    not_removed_snap.append(lv)
                else:
                    removed_snap.append(lv)
        return removed_snap, not_removed_snap


class DD():
    def __init__(self, method, bs, hash_method):
        self.hash_method = hash_method
        self.bs = bs
        if (method == 'full') or (method == 'diff'):
            self.method = method
        else:
            raise DDWrongMethod('Method', method)

    def create_full(self, source, dest):
        self.source_file = open(source, "rb")
        self.dest_file = open(dest, "wb")
        while True:
            try:
                data = next(self.__read_file())
            except StopIteration:
                break
            self.__write_file(data)

    def create_diff(self, source, dest):
        self.source_file = open(source, "rb")
        self.dest_file = open(dest, "wb")
        self.meta_file = open(dest + '.mt', "w", encoding='ASCII')
        while True:
            try:
                data = next(self.__read_file())
            except StopIteration:
                break
            self.__write_file(data, block_source=1)

    def __read_file(self):
        while True:
            chunk = self.source_file.read(self.bs)
            if chunk:
                yield chunk
            else:
                break

    def __read_metadata(self):
        pass

    def __write_file(self, data, block_source=None):
        """
\       Парраметр block_source передается только при создании дифференциального бэкапа.
        Он определяет из какого файла следует брать блок при восстановлении.
        0 - конец файла
        -1 - из фулла.
        +1 - из диффа.
        """
        self.dest_file.write(data)
        if block_source is not None:
            if self.hash_method == 'md5':
                data_hash = int(hashlib.md5(data).hexdigest(), base=16)
            elif self.hash_method == 'crc32':
                data_hash = int(zlib.crc32(data))
            elif self.hash_method == 'sha1':
                data_hash = int(hashlib.sha1(data).hexdigest(), base=16)
            else:
                data_hash = 1
            entry = str(block_source * data_hash) + '\n'
            self.meta_file.write(entry)


cloner = DD('full', bs=4096, hash_method='sha1')
#cloner.create_full('/tmp/test.log', '/tmp/dest.dd')
cloner.create_diff(source='/tmp/test.log', dest='/tmp/beta')