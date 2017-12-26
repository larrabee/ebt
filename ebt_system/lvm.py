from ebt_system import popen as _popen
import os
import sys
import logging

log = logging.getLogger('__main__')


def create_snapshot(source, size='10G', snap_suff='-snap'):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(snap_suff, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('snap_suff', __name__, sys._getframe().f_code.co_name)
    assert isinstance(size, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('size', __name__, sys._getframe().f_code.co_name)
    snap_name = source + snap_suff
    command = 'lvcreate --size {0} -A y --snapshot --name {1} {2}'.format(size, snap_name, source)
    log.info('Create snapshot of {0} with name {1} and size {2}'.format(source, snap_name, size))
    _popen(command=command)


def remove_snap_if_exist(source, snap_suff='-snap'):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(snap_suff, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('snap_suff', __name__, sys._getframe().f_code.co_name)
    snap_name = source + snap_suff
    if os.path.exists(snap_name):
        command = 'lvremove -A y {0} --force'.format(snap_name)
        log.info('Remove snapshot {0} of volume {1}'.format(snap_name, source))
        _popen(command=command)
    else:
        log.info('Snapshot with name {0} not found. Skipping remove.'.format(snap_name))


def remove_snap(source, snap_suff='-snap'):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(snap_suff, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('snap_suff', __name__, sys._getframe().f_code.co_name)
    snap_name = source + snap_suff
    command = 'lvremove -A y {0} --force'.format(snap_name)
    log.info('Remove snapshot {0} of volume {1}'.format(snap_name, source))
    _popen(command=command)
