from ebt_system import popen as __popen
import logging
import sys
from multiprocessing import cpu_count


log = logging.getLogger('__main__')


def create(source, dest, bs='8M', compress_level=0, compress_threads=cpu_count(), passwd=None):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('dest', __name__, sys._getframe().f_code.co_name)
    assert isinstance(bs, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('bs', __name__, sys._getframe().f_code.co_name)

    
    if compress_level > 0:
        command = 'dd if={0} bs={1} |pigz -c -{2} -p {3}'.format(source, bs, compress_level, compress_threads)
    else:
        command = 'dd if={0} bs={1}'.format(source, bs)

    if passwd is not None:
        command = '{0} |openssl enc -aes-256-cbc -k "{1}"'.format(command, passwd)

    command = '{0} |dd of={1} bs={2}'.format(command, dest, bs)

    __popen(command, shell=True)
    log.info('Successful create copy of {0} to {1}'.format(source, dest))
