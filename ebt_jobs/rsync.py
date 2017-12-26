import logging
import ebt_cleaner
import ebt_system
import ebt_files
import os

log = logging.getLogger('__main__')


class RsyncBackupFull(object):
    def __init__(self, source, dest_dir, day_exp, store_last, exclude=list(), include=list()):
        self.include = include
        self.exclude = exclude
        self.store_last = store_last
        self.day_exp = day_exp
        self.dest_dir = dest_dir
        self.source = source

    def _set_backup_dest(self):
        self.backup_date = ebt_cleaner.get_dir_name()
        self.dest = "{0}/{1}".format(self.dest_dir, self.backup_date)

    def _cleanup_old_backups(self):
        old_backups = ebt_cleaner.filter_list(path=self.dest_dir, dayexp=self.day_exp, store_last=self.store_last)
        ebt_system.rm(old_backups)

    def _pre_backup(self):
        pass

    def _post_backup(self):
        pass

    def _create_instance_backup(self):
        os.makedirs(self.dest)
        ebt_files.rsync.full_copy(source=self.source, dest=self.dest, exclude=self.exclude, include=self.include)

    def start(self):
        self._set_backup_dest()
        self._cleanup_old_backups()
        self._pre_backup()
        self._create_instance_backup()
        self._post_backup()


class RsyncBackupDiff(RsyncBackupFull):
    def _set_backup_dest(self):
        self.backup_date = ebt_cleaner.get_dir_name()
        self.full = ebt_cleaner.last_backup(self.dest_dir)
        self.dest = "{0}/{1}%{2}".format(self.dest_dir, self.backup_date, str(self.full).split('/')[-1])

    def _cleanup_old_backups(self):
        old_backups = ebt_cleaner.filter_list(path=self.dest_dir, dayexp=self.day_exp, store_last=self.store_last, fmt='%date%%%fdate')
        ebt_system.rm(old_backups)

    def _create_instance_backup(self):
        os.makedirs(self.dest)
        ebt_files.rsync.diff_copy(source=self.source, full=self.full, dest=self.dest, exclude=self.exclude, include=self.include)
