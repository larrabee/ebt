# Enhanced Backup Tool

## Overview
This is backup framework for creating flexible backup scripts.

## Features
1. Module for upload backups to Amazon Glacier and remove old Glacier backups.
2. MySQL module support creating backup with mysqldump and InnoBackupEx.
3. LVM module for snapshots.
4. Full/Diff backups of files backups with rsync.
5. Full/Diff backups of btrfs with btrfs send/receive.
6. Full/Diff backups of any binary files or block devices with diff-dd.
7. Full/Diff backups of Libvirt VM's.
8. Full/Diff backups of Libvirt VM's with external snapshots (works with qcow, qcow2, and raw file block devices).
9. Predefined jobs for popular cases.

## Install
1. Install following dependency with your OS package manager:
```
rsync
lvm2
p7zip
btrfs-progs
libvirt-python
MySQL-python
Percona-Server-client-57 (or another mysql client like mariadb-client)
percona-xtrabackup-24
pigz
python2-boto
python-lxml
python-pip
```
2. Install python package from pip:
```
pip install ebt
```

## Configure
Configuration files stored in `/etc/ebt/` directory.
File `/etc/ebt/ebt.conf` contain logging configuration.

File `/etc/ebt/plans.py` contains your backups jobs. Few examples of `plans.py`:

```python
import ebt_jobs.btrfs


def btrfs_root_full():
    backup = ebt_jobs.btrfs.BTRFSBackupFull(source='/', snap_dir='/.snap', dest_dir='/mnt/backup', day_exp=None, store_last=1)
    backup.start()

def btrfs_root_diff():
    backup = ebt_jobs.btrfs.BTRFSBackupDiff(source='/', snap_dir='/.snap', dest_dir='/mnt/backup', day_exp=None, store_last=5)
    backup.start()
```
This example of using predefined job `ebt_jobs.btrfs.BTRFSBackupDiff`. You can see all jobs in `ebt_jobs/` directory.

Next example. You can inheritance your own class from base class:
```python
import ebt_jobs.btrfs
import ebt_system

class BTRFSBackupFullWithRsync(ebt_jobs.btrfs.BTRFSBackupFull):
    def _post_backup(self):
        ebt_system.popen(command="rsync /usr/bin/rsync {0} remote:/mnt/backup/archive/".format(self.dest), shell=True)

class BTRFSBackupDiffWithRsync(ebt_jobs.btrfs.BTRFSBackupDiff, BTRFSBackupFullWithRsync):
    pass

def btrfs_root_full():
    backup = BTRFSBackupFullWithRsync(source='/', snap_dir='/.snap', dest_dir='/mnt/backup', day_exp=None, store_last=1)
    backup.start()

def btrfs_root_diff():
    backup = BTRFSBackupDiffWithRsync(source='/', snap_dir='/.snap', dest_dir='/mnt/backup', day_exp=None, store_last=5)
    backup.start()
```
You can write your own jobs from primitives that can be found in `ebt_*` dirs.

## EBT cli Usage
```
root » ebt --help
usage: ebt [-h] [-j JOBS [JOBS ...]] [-p PLAN] [-c CONFIG] [-v]

optional arguments:
  -h, --help            show this help message and exit
  -j JOBS [JOBS ...], --jobs JOBS [JOBS ...]
                        List of jobs to run
  -p PLAN, --plan PLAN  Custom path to plans file
  -c CONFIG, --config CONFIG
                        Custom path to config file
  -v, --version         Display program version and exit
```
Where `-j or --jobs` is the functions names from your `plans.py` file. Example: `ebt -j btrfs_root_full btrfs_root_diff`
If you specified few jobs they will be executed in sequential order.

## Diff-dd cli Usage
You can use for restoring diff backups of Libvirt VM's or for creating diff backups from external scripts.

Backup examples:
```
diff-dd --if /backup/full_disk_copy.raw --df /dev/lvm/changed_disk_snapshot --of /backup/diff_disk_copy.ddd
diff-dd --if /backup/full_disk_copy.raw --df /dev/lvm/changed_disk_snapshot > /backup/diff_disk_copy.ddd
diff-dd --if /backup/full_disk_copy.raw --df /dev/lvm/changed_disk_snapshot | gzip > /backup/diff_disk_copy.ddd.gz
diff-dd --if <(zcat /backup/full_disk_copy.raw.gz) --df /dev/lvm/changed_disk_snapshot --of /backup/diff_disk_copy.ddd
zcat /backup/full_disk_copy.raw.gz | ddd --df /dev/lvm/changed_disk_snapshot --of /backup/diff_disk_copy.ddd
cat /dev/lvm/changed_disk_snapshot | diff-dd --if <(zcat /backup/full_disk_copy.raw.gz) --of /backup/diff_disk_copy.ddd
diff-dd --if <(ssh remotehost cat /backup/full_disk_copy.raw) --df <(ssh remote2host cat /dev/lvm/changed_disk_snapshot) | ssh remote3host dd of=/backup/diff_disk_copy.ddd
```

Restore examples:
```
diff-dd --mode restore --if /backup/full_disk_copy.raw --df /backup/diff_disk_copy.ddd --of /dev/lvm/disk
diff-dd --mode restore --if <(zcat /backup/full_disk_copy.raw.gz) --df <(zcat /backup/diff_disk_copy.ddd.gz) > /dev/lvm/disk
zcat /backup/full_disk_copy.raw.gz | diff-dd --mode restore --df <(ssh remotehost cat /backup/diff_disk_copy.ddd.gz) | ssh remote2host dd of=/dev/lvm/disk
```

## Diff-dd API Usage
Backup:
```
import ebt_files
import gzip
iffd = gzip.open('/backup/full_disk_copy.raw.gz', 'rb')
dffd = open('/dev/lvm/changed_disk_snapshot', 'rb')
offd = gzip.open('/backup/diff_disk_copy.ddd', 'wb')

differ = ebt_files.ddd.CreateDiff(iffd=iffd, dffd=dffd, offd=offd, block_size=16384)
differ.start()
```

Restore:
```
import ebt_files
iffd = gzip.open('/backup/full_disk_copy.raw.gz', 'rb')
dffd = gzip.open('/backup/diff_disk_copy.ddd', 'rb')
offd = open('/dev/lvm/disk', 'wb')

differ_restore = ebt_files.ddd.RestoreDiff(iffd=iffd, dffd=dffd, offd=offd, block_size=16384)
differ_restore.start()
```

## License
GPLv3
