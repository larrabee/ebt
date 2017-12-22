import logging


log = logging.getLogger('__main__')


class MySQLDump:
    def __init__(self, instances, dest):
        self.instances = instances
        self.dest = dest

    def __prepare_dirs(self):
        pass

    def __cleanup_old_backups(self):
        pass

    def __create_instance_backup(self):
        pass

    def start(self):
        pass
