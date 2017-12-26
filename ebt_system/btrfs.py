import logging
import sys
from ebt_system import popen as _popen
from multiprocessing import cpu_count

log = logging.getLogger('__main__')


def subvolume_list(path):
    assert isinstance(path, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('path', __name__, sys._getframe().f_code.co_name)
    command = 'btrfs subvolume list {0}'.format(str(path))
    raw_output = _popen(command)[0]
    form_output = raw_output.decode()
    output = list()
    for string in form_output.split(sep='\n'):
        string = string.split(sep=' ')
        if len(string) > 1:
            output_element = {'id': string[1], 'gen': string[3], 'parrent': string[6], 'path': string[8]}
            output.append(output_element)
    return output


def subvolume_delete(path):
    assert isinstance(path, str) or isinstance(path, list), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('path', __name__, sys._getframe().f_code.co_name)
    if type(path) is str:
        command = 'btrfs subvolume delete {0}'.format(path)
        _popen(command)
        log.info('Delete btrfs subvolume {0}'.format(path))
    else:
        for subvolume in path:
            command = 'btrfs subvolume delete {0}'.format(subvolume)
            _popen(command)
            log.info('Delete btrfs subvolume {0}'.format(subvolume))


def subvolume_create_snapshot(source, dest, readonly=True):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('dest', __name__, sys._getframe().f_code.co_name)
    assert isinstance(readonly, bool), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('readonly', __name__, sys._getframe().f_code.co_name)
    command = 'btrfs subvolume snapshot'
    if readonly:
        command = '{0} -r'.format(command)
    command = "{0} {1} {2}".format(command, source, dest)
    _popen(command)
    log.info('Create snapshot of {0} to {1} , readonly: {2}'.format(source, dest, str(readonly)))


def file_create_snapshot(source, dest):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('dest', __name__, sys._getframe().f_code.co_name)
    command = 'cp --reflink {0} {1}'.format(source, dest)
    _popen(command)
    log.info('Create snapshot of file {0} to {1}'.format(source, dest))


def subvolume_send(source, dest, parent_path=None, compress_level=0, compress_threads=cpu_count()):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('dest', __name__, sys._getframe().f_code.co_name)
    assert (compress_level is None) or (isinstance(compress_level, int) and (compress_level in range(0, 9))), \
        '{1}.{2}: variable "{0}" has wrong type.'.format('compress', __name__, sys._getframe().f_code.co_name)
    assert isinstance(parent_path, str) or (parent_path is None), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('parent_path', __name__, sys._getframe().f_code.co_name)
    if parent_path is not None:
        command = 'btrfs send -p {0} {1}'.format(parent_path, source)
    else:
        command = 'btrfs send {0}'.format(source)
    if compress_level > 0:
        command = "{0} |pigz -c -{1} -p {2}".format(command, compress_level, compress_threads)
    command = "{0} > {1}".format(command, dest)
    _popen(command, shell=True)
    log.info('Send subvolume {0} to {1} , parrent: {2}'.format(source, dest, str(parent_path)))


