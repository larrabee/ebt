import modules.sys_mod
import sys
import logging

log = logging.getLogger('__main__')


def compress_7z(source, dest, level=5, password=None):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'.format('source', __name__,
                                                                                     sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'.format('dest', __name__,
                                                                                   sys._getframe().f_code.co_name)
    assert isinstance(level, int), '{1}.{2}: variable "{0}" has wrong type.'.format('full', __name__,
                                                                                    sys._getframe().f_code.co_name)
    assert isinstance(password, str) or isinstance(password, None), '{1}.{2}: variable "{0}" has wrong type.'.format(
        'full', __name__, sys._getframe().f_code.co_name)

    command = '7z -mhe=on -t7z -mx={0}'.format(str(level))
    if password is not None:
        command += ' -p{0}'.format(password)
    command += ' {0} {1}'.format(dest, source)
    modules.sys_mod.popen(command, logging_commands=False)
    log.info('Successful create 7z archive from {1} to {0}'.format(dest, source))
    modules.sys_mod.popen()
