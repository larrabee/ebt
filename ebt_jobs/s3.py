from ebt_cloud import S3
import logging
import ebt_cleaner
import ebt_system
import re
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
        self.workers = 64
        self.queue_length = 65536
        self.max_keys = 10000

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

    def _exclude_file_by_regex(self, file):
        if len(self.exclude) == 0:
            return False
        for regex in self.exclude:
            if re.match(regex, file.name) is not None:
                return True
        return False

    def _exclude_files_by_time(self, file):
        return False

    def _create_backup(self):
        log.info('Starting backup of bucket "{0}"'.format(self.bucket))
        files = []
        files_count = 0
        for file in self.s3.list_bucket(self.bucket, max_keys=self.max_keys):
            if self._exclude_files_by_time(file) is True:
                continue
            elif self._exclude_file_by_regex(file) is True:
                continue
            if len(files) < self.queue_length:
                files.append(file)
                files_count += 1
            else:
                files.append(file)
                files_count += 1
                self.s3.dump_files(files, self.dest, workers=self.workers)
                files = []
        self.s3.dump_files(files, self.dest, workers=self.workers)
        log.info('Backup of bucket "{0}" successfully completed. Backuped {1} files.'.format(self.bucket, files_count))

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

    def _exclude_files_by_time(self, file):
        if file.last_modified_dt > self.full_backup_date:
                    return False
        return True


class S3BackupFullS3sync(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket, dest_dir, day_exp, store_last, endpoint, prefix="", proto="s3st://", onfail="fatal"):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.dest_dir = dest_dir
        self.bucket = bucket
        self.endpoint = endpoint
        self.day_exp = day_exp
        self.store_last = store_last
        self.prefix = prefix
        self.workers = 256
        self.retry = 5
        self.disable_http2 = False
        self.debug = False
        self.proto = proto
        self.onfail = onfail

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

    def _create_backup(self):
        log.info('Starting backup of bucket "{0}" with s3sync'.format(self.bucket))
        command = 's3sync --sk {aws_access_key_id} --ss {aws_secret_access_key} --se {endpoint} -w {workers} -f {onfail} -r {retry} {proto}{bucket}{prefix} {dest}'.format(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint=self.endpoint,
            workers=self.workers,
            onfail=self.onfail,
            retry=self.retry,
            bucket=self.bucket,
            prefix=self.prefix,
            dest=self.dest,
            proto=self.proto,
        )
        if self.disable_http2:
            command = "{cmd} --disable-http2".format(cmd=command)
        if self.debug:
            command = "{cmd} --debug".format(cmd=command)
            
        exitcode, output = ebt_system.popen(command)
        log.info(output)
        log.info('Backup of bucket "{0}" successfully completed.'.format(self.bucket))

    def start(self):
        self._set_backup_dest()
        self._cleanup_old_backups()
        self._pre_backup()
        self._create_backup()
        self._post_backup()


class S3BackupDiffS3sync(S3BackupFullS3sync):
    def _set_backup_dest(self):
        self.backup_date = ebt_cleaner.get_dir_name()
        self.full = ebt_cleaner.last_backup(self.dest_dir)
        self.dest = "{0}/{1}%{2}".format(self.dest_dir, self.backup_date, str(self.full).split('/')[-1])
        self.full_backup_date = datetime.datetime.strptime(str(self.full).split('/')[-1], '%d-%m-%Y_%H:%M')

    def _cleanup_old_backups(self):
        old_backups = ebt_cleaner.filter_list(path=self.dest_dir, dayexp=self.day_exp, store_last=self.store_last, fmt='%date%%%fdate')
        ebt_system.rm(old_backups)

    def _exclude_files_by_time(self, file):
        if file.last_modified_dt > self.full_backup_date:
                    return False
        return True

    def _create_backup(self):
        log.info('Starting backup of bucket "{0}" with s3sync'.format(self.bucket))
        command = 's3sync --sk {aws_access_key_id} --ss {aws_secret_access_key} --se {endpoint} -w {workers} -r {retry} --ft {timestamp} {proto}{bucket}{prefix} {dest}'.format(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint=self.endpoint,
            workers=self.workers,
            onfail=self.onfail,
            retry=self.retry,
            bucket=self.bucket,
            prefix=self.prefix,
            dest=self.dest,
            proto=self.proto,
            timestamp=self.full_backup_date.strftime("%s"),
        )
        if self.disable_http2:
            command = "{cmd} --disable-http2".format(cmd=command)
        if self.debug:
            command = "{cmd} --debug".format(cmd=command)
        
        exitcode, output = ebt_system.popen(command)
        log.info(output)
        log.info('Backup of bucket "{0}" successfully completed.'.format(self.bucket))
