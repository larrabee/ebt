import logging
import sys
import shutil
import os
import subprocess
import errno

log = logging.getLogger('__main__')


def popen(command, logging_commands=True, shell=False, executable='/bin/bash', cwd='./', valid_exitcodes=None):
    if valid_exitcodes is None:
        valid_exitcodes = [0, ]
    assert isinstance(command, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('command', __name__, sys._getframe().f_code.co_name)
    assert isinstance(logging_commands, bool), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('command', __name__, sys._getframe().f_code.co_name)
    if logging_commands:
        log.debug('Exec command: {0}'.format(command))
    if shell:
        process = subprocess.Popen(command, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=shell,
                                   executable=executable, cwd=cwd)
    else:
        process = subprocess.Popen(command.split(), stderr=subprocess.STDOUT, stdout=subprocess.PIPE, cwd=cwd)
    output = process.communicate()[0]
    log.debug('Exit code: {0}, output: {1}'.format(process.returncode, output))
    if process.returncode not in valid_exitcodes:
        log.error('External program exit with invalid exit code.')
        if logging_commands:
            raise RuntimeError('Error code:', str(process.returncode), 'Output: ', output, 'Command: ', command)
        else:
            raise RuntimeError('Error code:', str(process.returncode), 'Output: ', output)
    return process.returncode, output


def rm(path):
    assert isinstance(path, str) or isinstance(path, list), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('path', __name__, sys._getframe().f_code.co_name)
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


def makedirs(path, exist_ok=False):
    if exist_ok is True:
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise
    else:
        os.makedirs(path)