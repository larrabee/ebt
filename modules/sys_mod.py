__author__ = 'larrabee'
import logging
import sys
import shutil
import os
import subprocess
import inspect


class Sys:
    def __init__(self):
        self.log = logging.getLogger('__main__')

    def popen(self, command, logging_commands=True, shell=False, executable='/bin/bash', cwd='./'):
        assert isinstance(command, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('command', __name__, sys._getframe().f_code.co_name)
        assert isinstance(logging_commands, bool), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('command', __name__, sys._getframe().f_code.co_name)
        if logging_commands:
            self.log.debug('Exec command: {0}'.format(command))
        if shell:
            process = subprocess.Popen(command, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=shell, executable=executable, cwd=cwd)
        else:
            process = subprocess.Popen(command.split(), stderr=subprocess.STDOUT, stdout=subprocess.PIPE, cwd=cwd)
        output = process.communicate()[0]
        self.log.debug('Exit code: {0}, output: {1}'.format(process.returncode, output))
        if process.returncode != 0:
            self.log.error('External program exit with not zero code.')
            if logging_commands:
                raise RuntimeError('Error code:', str(process.returncode), 'Output: ', output, 'Command: ', command)
            else:
                raise RuntimeError('Error code:', str(process.returncode), 'Output: ', output)
        return output

    def rm(self, path):
        assert isinstance(path, str) or isinstance(path, list), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('path', __name__, sys._getframe().f_code.co_name)
        if type(path) is str:
            if os.path.isdir(path):
                shutil.rmtree(path)
                self.log.info('Remove directory: {0}'.format(path))
            elif os.path.isfile(path):
                os.remove(path)
                self.log.info('Remove file: {0}'.format(path))
        else:
            for item in path:
                if os.path.isdir(item):
                    shutil.rmtree(item)
                    self.log.info('Remove directory: {0}'.format(item))
                elif os.path.isfile(item):
                    os.remove(item)
                    self.log.info('Remove file: {0}'.format(item))

    def getfunctions(self, function):
        functions_raw = inspect.getmembers(function, inspect.isfunction)
        functions = list()
        for item in functions_raw:
            functions.append(item[0])
        return functions


class Btrfs:
    def __init__(self):
        self.log = logging.getLogger('__main__')

    def sub_list(self, path):
        assert isinstance(path, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('path', __name__, sys._getframe().f_code.co_name)
        command = 'btrfs subvolume list {0}'.format(str(path))
        raw_output = Sys().popen(command)[0]
        form_output = raw_output.decode()
        output = list()
        for string in form_output.split(sep='\n'):
            string = string.split(sep=' ')
            if len(string) > 1:
                output_element = {'id': string[1], 'gen': string[3], 'parrent': string[6], 'path': string[8]}
                output.append(output_element)
        return output

    def sub_del(self, path):
        assert isinstance(path, str) or isinstance(path, list), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('path', __name__, sys._getframe().f_code.co_name)
        if type(path) is str:
            command = 'btrfs subvolume delete {0}'.format(path)
            Sys().popen(command)
            self.log.info('Delete btrfs subvolume {0}'.format(path))
        else:
            for subvolume in path:
                command = 'btrfs subvolume delete {0}'.format(subvolume)
                Sys().popen(command)
                self.log.info('Delete btrfs subvolume {0}'.format(subvolume))

    def sub_snap(self, source, dest, readonly=True):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('dest', __name__, sys._getframe().f_code.co_name)
        assert isinstance(readonly, bool), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('readonly', __name__, sys._getframe().f_code.co_name)
        command = 'btrfs subvolume snapshot'
        if readonly:
            command = '{0} -r'.format(command)
        command = '{0} "{1}" "{2}"'.format(command, source, dest)
        Sys().popen(command)
        self.log.info('Create snapshot of {0} to {1} , readonly: {2}'.format(source, dest, str(readonly)))

    def send(self, source, dest, parrent_path=None, compress=False):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('dest', __name__, sys._getframe().f_code.co_name)
        assert isinstance(compress, bool), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('compress', __name__, sys._getframe().f_code.co_name)
        assert isinstance(parrent_path, str) or (
        parrent_path is None), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('parrent_path', __name__, sys._getframe().f_code.co_name)
        command = 'btrfs send {0}'.format(source)
        if parrent_path is not None:
            command += ' -p {0}'.format(parrent_path)
        if compress is True:
            command += " |pigz -c"
        command += " > {0}".format(dest)
        Sys().popen(command, shell=True)
        self.log.info('Send subvolume {0} to {1} , parrent: {2}'.format(source, dest, str(parrent_path)))

    def file_snap(self, source, dest):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('dest', __name__, sys._getframe().f_code.co_name)
        command = 'cp --reflink {0} {1}'.format(source, dest)
        Sys().popen(command)
        self.log.info('Create snapshot of file {0} to {1} , readonly: {2}'.format(source, dest))


class LVM:
    def __init__(self):
        self.log = logging.getLogger('__main__')

    def create_snapshot(self, source, size='10G', snap_suff='-snap'):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(snap_suff, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('snap_suff', __name__, sys._getframe().f_code.co_name)
        assert isinstance(size, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('size', __name__, sys._getframe().f_code.co_name)
        snap_name = source + snap_suff
        command = 'lvcreate --size {0} -A y --snapshot --name {1} {2}'.format(size, snap_name, source)
        self.log.info('Create snapshot of {0} with name {1} and size {2}'.format(source, snap_name, size))
        Sys().popen(command=command)

    def remove_snap_if_exist(self, source, snap_suff='-snap'):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(snap_suff, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('snap_suff', __name__, sys._getframe().f_code.co_name)
        snap_name = source + snap_suff
        if os.path.exists(snap_name):
            command = 'lvremove -A y {0} --force'.format(snap_name)
            self.log.info('Remove snapshot {0} of volume {1}'.format(snap_name, source))
            Sys().popen(command=command)
        else:
            self.log.info('Snapshot with name {0} not found. Skipping remove.'.format(snap_name))

    def remove_snap(self, source, snap_suff='-snap'):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(snap_suff, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('snap_suff', __name__, sys._getframe().f_code.co_name)
        snap_name = source + snap_suff
        command = 'lvremove -A y {0} --force'.format(snap_name)
        self.log.info('Remove snapshot {0} of volume {1}'.format(snap_name, source))
        Sys().popen(command=command)
    
    def get_device_size(self, block_device):
        assert isinstance(block_device, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('block_device', __name__, sys._getframe().f_code.co_name)
        command = 'blockdev --getsize64 {0}'.format(block_device)
        size = str(Sys().popen(command=command)[0]).replace('\n', '')
        return int(size)
        
