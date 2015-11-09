#!/usr/bin/python3
__author__ = 'larrabee'
import modules.system
import sys
import logging


log = logging.getLogger('__main__')


def sub_list(path):
    assert isinstance(path, str), '{1}.{2}: variable "{0}" has wrong type.'.format('path', __name__,
                                                                                   sys._getframe().f_code.co_name)
    command = 'btrfs subvolume list {0}'.format(str(path))
    raw_output = modules.system.popen(command)[0]
    form_output = raw_output.decode()
    output = list()
    for string in form_output.split(sep='\n'):
        string = string.split(sep=' ')
        if len(string) > 1:
            output_element = {'id': string[1], 'gen': string[3], 'parrent': string[6], 'path': string[8]}
            output.append(output_element)
    return output


def sub_del(path):
    assert isinstance(path, str) or isinstance(path, list), '{1}.{2}: variable "{0}" has wrong type.'.format('path', __name__,
                                                                                   sys._getframe().f_code.co_name)
    if type(path) is str:
        command = 'btrfs subvolume delete {0}'.format(path)
        modules.system.popen(command)
        log.info('Delete btrfs subvolume {0}'.format(path))
    else:
        for subvolume in path:
            command = 'btrfs subvolume delete {0}'.format(subvolume)
            modules.system.popen(command)
            log.info('Delete btrfs subvolume {0}'.format(subvolume))


def sub_snap(source, dest, readonly=True):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'.format('source', __name__,
                                                                                     sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'.format('dest', __name__,
                                                                                   sys._getframe().f_code.co_name)
    assert isinstance(readonly, bool), '{1}.{2}: variable "{0}" has wrong type.'.format('readonly', __name__,
                                                                                        sys._getframe().f_code.co_name)
    command = 'btrfs subvolume snapshot {0} {1}'.format(source, dest)
    if readonly:
        command += ' -r'
    modules.system.popen(command)
    log.info('Create snapshot of {0} to {1} , readonly: {2}'.format(source, dest, str(readonly)))


def send(source, dest, parrent_path=None):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'.format('source', __name__,
                                                                                     sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'.format('dest', __name__,
                                                                                   sys._getframe().f_code.co_name)
    assert isinstance(parrent_path, str) or (parrent_path is None), '{1}.{2}: variable "{0}" has wrong type.'.format('parrent_path',
                                                                                                   __name__,
                                                                                                   sys._getframe().f_code.co_name)
    command = 'btrfs send {0} -f {1}'.format(source, dest)
    if parrent_path is not None:
        command += ' -p {0}'.format(parrent_path)
    modules.system.popen(command)
    log.info('Send subvolume {0} to {1} , parrent: {2}'.format(source, dest, str(parrent_path)))


def file_snap(source, dest):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'.format('source', __name__,
                                                                                     sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'.format('dest', __name__,
                                                                                   sys._getframe().f_code.co_name)
    command = 'cp --reflink {0} {1}'.format(source, dest)
    modules.system.popen(command)
    log.info('Create snapshot of file {0} to {1} , readonly: {2}'.format(source, dest))
