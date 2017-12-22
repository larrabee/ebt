from ebt_system import popen
import logging
import sys


log = logging.getLogger('__main__')


def __list_to_string(file_list, mode):
    line = ''
    if file_list is not None:
        for item in file_list:
            line += ' --{0}=\'{1}\''.format(mode, item)
    return line


def full_copy(source, dest, rsync_options='aAX', include=None, exclude=None):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'\
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'\
        .format('dest', __name__, sys._getframe().f_code.co_name)
    assert isinstance(rsync_options, str), '{1}.{2}: variable "{0}" has wrong type.'\
        .format('rsync_options', __name__, sys._getframe().f_code.co_name)
    command = 'rsync -{0}{1}{2} {3} {4}'.format(rsync_options, __list_to_string(include, 'include'), __list_to_string(exclude, 'exclude'), source, dest)
    popen(command)
    log.info('Successful create full copy of {0} to {1}'.format(source, dest))


def diff_copy(source, full, dest, include=None, exclude=None):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'.format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(full, str), '{1}.{2}: variable "{0}" has wrong type.'.format('full', __name__, sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'.format('dest', __name__, sys._getframe().f_code.co_name)
    command = 'rsync -a{0}{1} --inplace --delete --backup  --link-dest={2} {3} {4}'.format(
        __list_to_string(include, 'include'), __list_to_string(exclude, 'exclude'), full, source, dest)
    popen(command)
    log.info('Successful create diff copy of {0} with full {1} to {2}'.format(source, full, dest))
