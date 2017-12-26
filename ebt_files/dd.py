from ebt_system import popen as __popen
import logging
import sys
from multiprocessing import cpu_count


log = logging.getLogger('__main__')


def create(source, dest, bs='8M', compress_level=0, compress_threads=cpu_count()):
    assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('source', __name__, sys._getframe().f_code.co_name)
    assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('dest', __name__, sys._getframe().f_code.co_name)
    assert isinstance(bs, str), '{1}.{2}: variable "{0}" has wrong type.' \
        .format('bs', __name__, sys._getframe().f_code.co_name)
    if compress_level > 0:
        command = 'dd if={0} bs={2} |pigz -c -{3} -p {4} |dd of={1} bs={2}'.format(source, dest, bs, compress_level,
                                                                                   compress_threads)
    else:
        command = 'dd if={0} of={1} bs={2}'.format(source, dest, bs)

    __popen(command, shell=True)
    log.info('Successful create copy of {0} to {1}'.format(source, dest))
