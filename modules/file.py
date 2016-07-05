__author__ = 'larrabee'
import modules.sys_mod
import sys
import logging


class Rsync:
    def __init__(self):
        self.include = None
        self.exclude = None
        self.log = logging.getLogger('__main__')

    def __get_include(self):
        assert not isinstance(self.include, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('include', __name__, sys._getframe().f_code.co_name)
        include = ''
        if self.include is not None:
            for item in self.include:
                include += ' --include=\'{0}\''.format(item)
        return include

    def __get_exclude(self):
        assert not isinstance(self.exclude, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('exclude', __name__, sys._getframe().f_code.co_name)
        exclude = ''
        if self.exclude is not None:
            for item in self.exclude:
                exclude += ' --exclude={0}'.format(item)
        return exclude

    def full_copy(self, source, dest, rsync_options='aAX'):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('dest', __name__, sys._getframe().f_code.co_name)
        assert isinstance(rsync_options, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('rsync_options', __name__, sys._getframe().f_code.co_name)
        command = 'rsync -{0}{1}{2} {3} {4}'.format(rsync_options, self.__get_include(), self.__get_exclude(), source, dest)
        modules.sys_mod.Sys().popen(command)
        self.log.info('Successful create full copy of {0} to {1}'.format(source, dest))

    def diff_copy(self, source, full, dest):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'.format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(full, str), '{1}.{2}: variable "{0}" has wrong type.'.format('full', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'.format('dest', __name__, sys._getframe().f_code.co_name)
        command = 'rsync -a{0}{1} --inplace --delete --backup  --link-dest={2} {3} {4} {5}'.format(
            self.__get_include(), self.__get_exclude(), full, source, dest)
        modules.sys_mod.Sys().popen(command)
        self.log.info('Successful create diff copy of {0} with full {1} to {2}'.format(source, full, dest))


class Compress:
    def __init__(self):
        self.log = logging.getLogger('__main__')

    def c7z(self, source, dest, level=5, password=None):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('dest', __name__, sys._getframe().f_code.co_name)
        assert isinstance(level, int), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('full', __name__, sys._getframe().f_code.co_name)
        assert isinstance(password, str) or isinstance(password, None), '{1}.{2}: variable "{0}" has wrong type.'\
            .format('full', __name__, sys._getframe().f_code.co_name)
        command = '7z -mhe=on -t7z -mx={0}'.format(str(level))
        if password is not None:
            command += ' -p{0}'.format(password)
        command += ' {0} {1}'.format(dest, source)
        modules.sys_mod.Sys().popen(command, logging_commands=False)
        self.log.info('Successful create 7z archive from {1} to {0}'.format(dest, source))

class DD:
    def __init__(self):
        self.log = logging.getLogger('__main__')

    def dd(self, source, dest, bs='2M'):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('dest', __name__, sys._getframe().f_code.co_name)
        assert isinstance(bs, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('bs', __name__, sys._getframe().f_code.co_name)
        command = 'dd if={0} of={1} bs={2}'.format(source, dest, bs)
        modules.sys_mod.Sys().popen(command)
        self.log.info('Successful create copy of {0} to {1}'.format(source, dest))

    def dd_with_compression(self, source, dest, bs='2M', compress_level=5):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('dest', __name__, sys._getframe().f_code.co_name)
        assert isinstance(bs, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('bs', __name__, sys._getframe().f_code.co_name)
        command = 'dd if={0} bs={2} |gzip -c -{3}| dd of={1} bs={2}'.format(source, dest, bs, compress_level)
        modules.sys_mod.Sys().popen(command, shell=True)
        self.log.info('Successful create compressed copy of {0} to {1}'.format(source, dest))
