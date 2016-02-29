__author__ = 'larrabee'
import modules.sys_mod
import sys
import logging


class Rsync():
    def __init__(self):
        self.include = None
        self.exclude = None
        self.log = logging.getLogger('__main__')


    def __get_include(self):
        assert not isinstance(self.include, str), '{1}.{2}: variable "{0}" has wrong type.'.format('include', __name__, sys._getframe().f_code.co_name)
        include = ''
        if self.include is not None:
            for item in self.include:
                include += ' --include=\'{0}\''.format(item)
        return include

    def __get_exclude(self):
        assert not isinstance(self.exclude, str), '{1}.{2}: variable "{0}" has wrong type.'.format('exclude', __name__, sys._getframe().f_code.co_name)
        exclude = ''
        if self.exclude is not None:
            for item in self.exclude:
                exclude += ' --exclude={0}'.format(item)
        return exclude

    def full_copy(self, source, dest, rsync_options='aAX'):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'.format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'.format('dest', __name__, sys._getframe().f_code.co_name)
        assert isinstance(rsync_options, str), '{1}.{2}: variable "{0}" has wrong type.'.format('rsync_options', __name__, sys._getframe().f_code.co_name)
        command = 'rsync -{0}{1}{2} {3} {4}'.format(rsync_options, self.__get_include(), self.__get_exclude(), source, dest)
        modules.sys_mod.popen(command)
        self.log.info('Successful create full copy of {0} to {1}'.format(source, dest))

    def diff_copy(self, source, full, dest):
        assert isinstance(source, str), '{1}.{2}: variable "{0}" has wrong type.'.format('source', __name__, sys._getframe().f_code.co_name)
        assert isinstance(full, str), '{1}.{2}: variable "{0}" has wrong type.'.format('full', __name__, sys._getframe().f_code.co_name)
        assert isinstance(dest, str), '{1}.{2}: variable "{0}" has wrong type.'.format('dest', __name__, sys._getframe().f_code.co_name)
        command = 'rsync -a{0}{1} --inplace --delete --backup  --link-dest={2} {3} {4} {5}'.format(
            self.__get_include(), self.__get_exclude(), full, source, dest)
        modules.sys_mod.popen(command)
        self.log.info('Successful create diff copy of {0} with full {1} to {2}'.format(source, full, dest))


class DD():
    def __init__(self):
        pass

