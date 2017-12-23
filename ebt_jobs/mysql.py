import logging
import ebt_cleaner
from ebt_system import rm
import ebt_db
import os

log = logging.getLogger('__main__')


class MySQLDump(object):
    def __init__(self, instances, dest_dir, day_exp=None, store_last=None):
        self.instances = instances
        self.dest_dir = dest_dir
        self.day_exp = day_exp
        self.store_last = store_last

    def _set_backup_dest(self):
        backup_date = ebt_cleaner.get_dir_name()
        self.dest = "{0}/{1}".format(self.dest_dir, backup_date)

    def _cleanup_old_backups(self):
        old_backups = ebt_cleaner.filter_list(path=self.dest_dir, dayexp=self.day_exp, store_last=self.store_last)
        rm(old_backups)

    def _pre_backup(self):
        pass

    def _post_backup(self):
        pass

    def _create_instance_backup(self, instance):
        instance['dest'] = "{0}/{1}".format(self.dest, instance['name'])
        client = ebt_db.Mysql(instance)
        log.info('Stop slave on server {0}'.format(instance['name']))
        client.slave_stop()
        log.info('Create dest dir: {0}'.format(instance['dest']))
        os.makedirs(instance['dest'])
        log.info('Start backup databases {0} from {1}'.format(instance['db'], instance['name']))
        client.slave_status_to_file()
        client.mysqldump()
        log.info('Backup databases {0} from {1} finished'.format(instance['db'], instance['name']))
        log.info('Start slave on server {0}'.format(instance['name']))
        client.slave_start()

    def start(self):
        self._set_backup_dest()
        self._cleanup_old_backups()
        self._pre_backup()
        for instance in self.instances:
            self._create_instance_backup(instance)
        self._post_backup()


class InnoBackupEX(MySQLDump):
    def _create_instance_backup(self, instance):
        instance['dest'] = "{0}/{1}".format(self.dest, instance['name'])
        client = ebt_db.Mysql(instance)
        log.info('Create dest dir: {0}'.format(instance['dest']))
        os.makedirs(instance['dest'])
        log.info('Start backup databases from {0}'.format(instance['name']))
        client.innobackupex()
        log.info('Backup databases from {0} finished'.format(instance['name']))
