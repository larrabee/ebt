#!/usr/bin/python
__author__ = 'larrabee'
import modules.cleaner
import modules.file
import modules.sys_mod
import modules.virtualization
import libvirt
import logging
import datetime
import os

log = logging.getLogger('__main__')



def vm_full(dest_dir, dayexp=30, store_last=5, exclude=list(), include=list()):
    backup_date = datetime.datetime.now().strftime('%d-%m-%Y_%H:%M')
    dest = dest_dir + '/' + backup_date
    #Delete old backups
    try:
        modules.sys_mod.Sys().rm(modules.cleaner.filter_list(path=dest_dir, dayexp=dayexp, store_last=store_last))
    except Exception as e:
        log.error('Delete old backups failed')
        log.debug(e)
    #Create Backup
    libvirt_mod = modules.virtualization.Libvirt()
    libvirt_mod.exclude = exclude
    libvirt_mod.include = include
    for domain in libvirt_mod.filter_domain_list(libvirt_mod.list_domains()):
        log.info('Start backup domain {0}'.format(domain.name()))
        os.makedirs('{0}/{1}'.format(dest, domain.name()))
        log.info('Export domain XML to {0}/{1}/{1}.xml'.format(dest, domain.name()))
        libvirt_mod.export_xml(domain=domain, path='{0}/{1}/{1}.xml'.format(dest, domain.name()))
        log.debug('Save memory image to {0}/{1}/memory.save'.format(dest, domain.name()))
        domain.save(to='{0}/{1}/memory.save'.format(dest, domain.name()))
        domain_disks = libvirt_mod.get_domain_disks(domain)
        for disk in domain_disks:
            if disk['source_type'] == 'dev':
                log.info('Create snapshot of disk {0}'.format(disk['path']))
                modules.sys_mod.LVM().remove_snap_if_exist(source=disk['path'])
                modules.sys_mod.LVM().create_snapshot(source=disk['path'])
            elif disk['source_type'] == 'file':
                log.info('Create copy of disk {3} to {0}/{1}/{2'.format(dest, domain.name(), os.path.basename(disk['path']), disk['path']))
                modules.file.Rsync().full_copy(source=disk['path'], dest='{0}/{1}/{2}'.format(dest, domain.name(), os.path.basename(disk['path'])))
        log.info('Restore memory from file {0}/{1}/memory.save'.format(dest, domain.name()))
        libvirt_mod.restore('{0}/{1}/memory.save'.format(dest, domain.name()))
        for disk in domain_disks:
            if disk['source_type'] == 'dev':
                log.info('Create copy of snapshot {3}-snap to {0}/{1}/{2}.img.gz'.format(dest, domain.name(), os.path.basename(disk['path']), disk['path']))
                modules.file.DD().dd_with_compression(source='{0}-snap'.format(disk['path']), dest='{0}/{1}/{2}.img.gz'.format(dest, domain.name(), os.path.basename(disk['path'])))
                log.info('Remove snapshot {0}-snap'.format(disk['path']))
                modules.sys_mod.LVM().remove_snap(disk['path'])

def backup_vm_daily():
    vm_full(dest_dir='/opt/backups/daily')

def backup_vm_weekly():
    vm_full(dest_dir='/opt/backups/weekly')
