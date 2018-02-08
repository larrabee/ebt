from ebt_cloud import S3
import logging
import ebt_cleaner
import ebt_system
import re
import copy
import datetime

log = logging.getLogger('__main__')


class S3BackupFull(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket, dest_dir, day_exp, store_last, exclude=[], **kwargs):
        self.s3 = S3(aws_access_key_id=aws_access_key_id,
                     aws_secret_access_key=aws_secret_access_key,
                     **kwargs
                     )
        self.dest_dir = dest_dir
        self.bucket = bucket
        self.exclude = exclude
        self.day_exp = day_exp
        self.store_last = store_last
        self.workers = 24


    def _set_backup_dest(self):
        backup_date = ebt_cleaner.get_dir_name()
        self.dest = "{0}/{1}".format(self.dest_dir, backup_date)

    def _cleanup_old_backups(self):
        old_backups = ebt_cleaner.filter_list(path=self.dest_dir, dayexp=self.day_exp, store_last=self.store_last)
        ebt_system.rm(old_backups)

    def _pre_backup(self):
        pass

    def _post_backup(self):
        pass

    def _exclude_files_by_regex(self, files):
        if len(self.exclude) == 0:
            return files
        filtered_files = copy.deepcopy(files)
        for file in files:
            for regex in self.exclude:
                if re.match(regex, file.name) is not None:
                    if file in filtered_files:
                        filtered_files.remove(file)
        return  filtered_files

    def _create_backup(self):
        files = self.s3.list_bucket(self.bucket)
        files = self._exclude_files_by_regex(files)
        self.s3.dump_files(files, self.dest, workers=self.workers)

    def start(self):
        self._set_backup_dest()
        self._cleanup_old_backups()
        self._pre_backup()
        self._create_backup()
        self._post_backup()


class S3BackupDiff(S3BackupFull):
    def _set_backup_dest(self):
        self.backup_date = ebt_cleaner.get_dir_name()
        self.full = ebt_cleaner.last_backup(self.dest_dir)
        self.dest = "{0}/{1}%{2}".format(self.dest_dir, self.backup_date, str(self.full).split('/')[-1])
        self.full_backup_date = datetime.datetime.strptime(str(self.full).split('/')[-1], '%d-%m-%Y_%H:%M')

    def _cleanup_old_backups(self):
        old_backups = ebt_cleaner.filter_list(path=self.dest_dir, dayexp=self.day_exp, store_last=self.store_last, fmt='%date%%%fdate')
        ebt_system.rm(old_backups)

    def _exclude_files_by_time(self, files):
        filtered_files = []
        for file in files:
            if file.last_modified_dt > self.full_backup_date:
                        filtered_files.append(file)
        return filtered_files

    def _create_backup(self):
        files = self.s3.list_bucket(self.bucket)
        files = self._exclude_files_by_time(files)
        files = self._exclude_files_by_regex(files)
        self.s3.dump_files(files, self.dest, workers=self.workers)