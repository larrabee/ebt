__author__ = 'larrabee'
import subprocess
import logging
import sys
import shutil
import os

log = logging.getLogger('__main__')


def popen(command):
    assert isinstance(command, str), '{1}.{2}: variable "{0}" has wrong type.'.format('command', __name__,
                                                                                   sys._getframe().f_code.co_name)
    log.debug('Exec command: {0}'.format(command))
    process = subprocess.Popen(command.split(), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    output = process.communicate()
    log.debug('Exit code: {0}, output: {1}'.format(process.returncode, output))
    if process.returncode != 0:
        log.error('External program exit with not zero code.')
        raise RuntimeError('Error code:', str(process.returncode), 'Output: ', output, 'Command: ', command)
    return output


def rm(path):
    assert isinstance(path, str) or isinstance(path, list), '{1}.{2}: variable "{0}" has wrong type.'.format('path', __name__,
                                                                                   sys._getframe().f_code.co_name)
    if type(path) is str:
        if os.path.isdir(path):
            shutil.rmtree(path)
            log.info('Remove directory: {0}'.format(path))
        elif os.path.isfile(path):
            os.remove(path)
            log.info('Remove file: {0}'.format(path))
    else:
        for item in path:
            if os.path.isdir(item):
                shutil.rmtree(item)
                log.info('Remove directory: {0}'.format(item))
            elif os.path.isfile(item):
                os.remove(item)
                log.info('Remove file: {0}'.format(item))