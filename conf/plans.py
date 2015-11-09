#!/usr/bin/python3
__author__ = 'larrabee'
import modules.btrfs
import modules.cleaner
import modules.copy
import modules.system
import logging
import datetime

log = logging.getLogger('__main__')


def images_full():
    source = '/.snap/test1'
    dest_dir = '/tmp/test'
    snap_dir = '/.snap'
    dayexp = 30
    store_last = 4
    ### Code ###
    backup_date = datetime.datetime.now().strftime('%d-%m-%Y_%H:%M')
    snap = snap_dir + '/' + backup_date
    dest = dest_dir + '/' + backup_date
    #Delete old backups and snapshots
    try:
        modules.btrfs.sub_del(modules.cleaner.filter_list(path=snap_dir, dayexp=dayexp, store_last=store_last))
    except Exception as e:
        log.error('Delete old snapshots failed')
        log.debug(e)
    try:
        modules.system.rm(modules.cleaner.filter_list(path=dest_dir, dayexp=dayexp, store_last=store_last))
    except Exception as e:
        log.error('Delete old backups failed')
        log.debug(e)
    #Create snapshot
    modules.btrfs.sub_snap(source=source, dest=snap, readonly=True)
    modules.btrfs.send(source=snap, dest=dest)


def images_diff():
    source = '/.snap/test1'
    dest_dir = '/tmp/test'
    snap_dir = '/.snap'
    dayexp = 30
    store_last = 4
    ### Code ###
    backup_date = datetime.datetime.now().strftime('%d-%m-%Y_%H:%M')
    snap = snap_dir + '/' + backup_date
    #Find last full backup
    full = modules.cleaner.last_backup(dest_dir)
    full_name = str(full).split(sep='/')[-1]
    dest = dest_dir + '/' + backup_date + '%' + full_name
    #Delete old backups and snapshots
    try:
        modules.btrfs.sub_del(modules.cleaner.filter_list(path=snap_dir, dayexp=dayexp, store_last=store_last))
    except Exception as e:
        log.error('Delete old snapshots failed')
        log.debug(e)
    try:
        modules.system.rm(modules.cleaner.filter_list(path=dest_dir, dayexp=dayexp, store_last=store_last, format='%date%%%fdate'))
    except Exception as e:
        log.error('Delete old backups failed')
        log.debug(e)
    #Create snapshot
    modules.btrfs.sub_snap(source=source, dest=snap, readonly=True)
    modules.btrfs.send(source=snap, dest=dest, parrent_path=snap_dir + '/' + full_name)