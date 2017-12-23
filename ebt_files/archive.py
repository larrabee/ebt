from ebt_system import popen as _popen
import logging
import sys

log = logging.getLogger('__main__')


def create7z(source, dest, level=5, password=None, options=None):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('dest', __name__, sys._getframe().f_code.co_name)
    assert isinstance(level, int), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('full', __name__, sys._getframe().f_code.co_name)
    assert isinstance(password, str) or (password is None), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('full', __name__, sys._getframe().f_code.co_name)
    assert isinstance(options, str) or (options is None), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('full', __name__, sys._getframe().f_code.co_name)
    command = '7za a -mhe=on -t7z -mx={0}'.format(str(level))
    if password is not None:
        command += ' -p{0}'.format(password)
    if options is not None:
        command += ' {0}'.format(options)
    command += ' {0} {1}'.format(dest, source)
    _popen(command, logging_commands=False)
    log.info('Successful create 7z archive from {1} to {0}'.format(dest, source))
