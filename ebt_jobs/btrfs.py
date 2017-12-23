import logging
import ebt_cleaner
import ebt_system
import ebt_files
import os

log = logging.getLogger('__main__')


class BTRFSBackupFull(object):
    def __init__(self, source, snap_dir, dest_dir, day_exp, store_last):
        self.store_last = store_last
        self.day_exp = day_exp
        self.dest_dir = dest_dir
        self.snap_dir = snap_dir
        self.source = source

    def __set_backup_dest(self):
        self.backup_date = ebt_cleaner.get_dir_name()
        self.dest = "{0}/{1}".format(self.dest_dir, self.backup_date)
        self.snap = "{0}/{1}".format(self.snap_dir, self.backup_date)

    def __cleanup_old_backups(self):
        old_backups = ebt_cleaner.filter_list(path=self.dest_dir, dayexp=self.day_exp, store_last=self.store_last)
        ebt_system.rm(old_backups)

    def __cleanup_old_snapshots(self):
        old_snapshots = ebt_cleaner.filter_list(path=self.snap_dir, dayexp=self.day_exp, store_last=self.store_last)
        ebt_system.rm(old_snapshots)

    def __pre_backup(self):
        pass

    def __post_backup(self):
        pass

    def __create_instance_backup(self, domain):
        ebt_system.btrfs.subvolume_create_snapshot(source=self.source, dest=self.snap, readonly=True)
        ebt_system.btrfs.subvolume_send(source=self.snap, dest=self.dest, compress_level=6)

    def start(self):
        self.__set_backup_dest()
        self.__cleanup_old_backups()
        self.__cleanup_old_snapshots()
        self.__pre_backup()
        self.__create_instance_backup()
        self.__post_backup()