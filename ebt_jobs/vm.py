import logging
import ebt_cleaner
import ebt_system
import ebt_files
import ebt_virt
import ebt_cloud
import os

log = logging.getLogger('__main__')


class LibvirtBackup(object):
    def __init__(self, dest_dir, dayexp=None, store_last=5, exclude_vm=list(), include_vm=list(), exclude_disks=list(),
                 dump_memory=True):
        self.dump_memory = dump_memory
        self.exclude_disks = exclude_disks
        self.store_last = store_last
        self.dest_dir = dest_dir
        self.dayexp = dayexp
        self.libvirt_client = ebt_virt.Libvirt()
        self.libvirt_client.exclude = exclude_vm
        self.libvirt_client.include = include_vm

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

    def _create_instance_backup(self, domain):
        log.info('Start backup domain {0}'.format(domain.name()))
        os.makedirs('{0}/{1}'.format(self.dest, domain.name()))
        log.info('Export domain XML to {0}/{1}/{1}.xml'.format(self.dest, domain.name()))
        self.libvirt_client.export_xml(domain=domain, path='{0}/{1}/{1}.xml'.format(self.dest, domain.name()))
        if (self.dump_memory is True) and (domain.isActive() == 1):
            log.debug('Save memory image to {0}/{1}/memory.save'.format(self.dest, domain.name()))
            domain.save(to='{0}/{1}/memory.save'.format(self.dest, domain.name()))
        elif (self.dump_memory is False) and (domain.isActive() == 1):
            log.debug('Suspending domain {0}'.format(domain.name()))
            domain.suspend()
        domain_disks = self.libvirt_client.get_domain_disks(domain)
        for disk in domain_disks:
            if (disk['source_type'] == 'dev') and (disk['path'] not in self.exclude_disks):
                log.info('Create snapshot of disk {0}'.format(disk['path']))
                ebt_system.lvm.remove_snap_if_exist(source=disk['path'])
                ebt_system.lvm.create_snapshot(source=disk['path'])
            elif (disk['source_type'] == 'file') and (disk['path'] not in self.exclude_disks):
                log.info(
                    'Create copy of disk {3} to {0}/{1}/{2}'.format(self.dest, domain.name(),
                                                                    os.path.basename(disk['path']),
                                                                    disk['path']))
                ebt_files.rsync.full_copy(source=disk['path'],
                                          dest='{0}/{1}/{2}'.format(self.dest, domain.name(),
                                                                    os.path.basename(disk['path'])))
        if (self.dump_memory is True) and (domain.isActive() == 0) and (
                    os.path.isfile("{0}/{1}/memory.save".format(self.dest, domain.name())) is True):
            log.info('Restore memory from file {0}/{1}/memory.save'.format(self.dest, domain.name()))
            self.libvirt_client.restore('{0}/{1}/memory.save'.format(self.dest, domain.name()))
        elif (self.dump_memory is False) and (domain.isActive() == 1):
            log.debug('Resuming domain {0}'.format(domain.name()))
            domain.resume()
        for disk in domain_disks:
            if (disk['source_type'] == 'dev') and (disk['path'] not in self.exclude_disks):
                log.info('Create copy of snapshot {3}-snap to {0}/{1}/{2}.img.gz'.format(self.dest, domain.name(),
                                                                                         os.path.basename(disk['path']),
                                                                                         disk['path']))
                ebt_files.dd.create(source='{0}-snap'.format(disk['path']),
                                    dest='{0}/{1}/{2}.img.gz'.format(self.dest, domain.name(), os.path.basename(disk['path'])),
                                    compress_level=6)
                open('{0}/{1}/{2}.img.size'.format(self.dest, domain.name(), os.path.basename(disk['path'])), mode='w').write(
                    str(self.libvirt_client.device_size(domain, disk['target'])))
                log.info('Remove snapshot {0}-snap'.format(disk['path']))
                ebt_system.lvm.remove_snap(disk['path'])

    def start(self):
        self._set_backup_dest()
        self._cleanup_old_backups()
        self._pre_backup()
        domains = self.libvirt_client.filter_domain_list(self.libvirt_client.list_domains())
        for domain in domains:
            self._create_instance_backup(domain)
        self._post_backup()


class LibvirtBackupToGlacier(LibvirtBackup):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, vault, archive_pass, *args, **kwargs):
        super(LibvirtBackupToGlacier, self).__init__(*args, **kwargs)
        self.glacier = ebt_cloud.Amazon(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                                       region_name=region_name)
        self.archive_pass = archive_pass
        self.vault = vault

    # noinspection PyMethodOverriding
    def _post_backup(self, domain):
        log.info('Compressing "{0}/{1}" to "{0}/{1}.7z"'.format(self.dest, domain.name()))
        ebt_files.archive.create7z(source="{0}/{1}".format(self.dest, domain.name()),
                                    dest="{0}/{1}.7z".format(self.dest, domain.name()), password=self.archive_pass,
                                    options="-sdel", level=1)
        log.info('Upload file "{0}/{1}.7z" to vault "{2}"'.format(dest, domain.name(), vault))
        archive_id = self.glacier.upload_file(vault_name=self.vault, description=domain.name(),
                                         path="{0}/{1}.7z".format(self.dest, domain.name()))
        log.info('Archive uploaded successfully. Archive id: "{0}"'.format(archive_id))
        log.info('Remove file "{0}/{1}.7z"'.format(self.dest, domain.name()))
        ebt_system.rm("{0}/{1}.7z".format(self.dest, domain.name()))

    def start(self):
        self._set_backup_dest()
        self._cleanup_old_backups()
        self._pre_backup()
        domains = self.libvirt_client.filter_domain_list(self.libvirt_client.list_domains())
        for domain in domains:
            self._create_instance_backup(domain)
        for domain in domains:
            self._post_backup(domain)


class LibvirtBackupExternalSnapshot(LibvirtBackup):
    def __init__(self, snap_dir, *args, **kwargs):
        super(LibvirtBackupExternalSnapshot, self).__init__(*args, **kwargs)
        self.snap_dir = snap_dir

    def _set_backup_dest(self):
        super(LibvirtBackupExternalSnapshot, self).__set_backup_dest()
        self.snap = "{0}/{1}".format(self.snap_dir, self.backup_date)

    def _cleanup_old_backups(self):
        super(LibvirtBackupExternalSnapshot, self).__cleanup_old_backups()
        if self.dest != self.snap:
            old_backups = ebt_cleaner.filter_list(path=self.snap_dir, dayexp=self.day_exp, store_last=self.store_last)
            ebt_system.rm(old_backups)

    def _create_instance_backup(self, domain):
        log.info('Start backup domain {0}'.format(domain.name()))
        os.makedirs('{0}/{1}'.format(self.dest, domain.name()))
        if self.dest != self.snap:
            os.makedirs('{0}/{1}'.format(self.snap, domain.name()))
        log.info('Export domain XML to {0}/{1}/{1}.xml'.format(self.dest, domain.name()))
        self.libvirt_client.export_xml(domain=domain, path='{0}/{1}/{1}.xml'.format(self.dest, domain.name()))
        if (self.dump_memory is True) and (domain.isActive() == 1):
            memory_path = '{0}/{1}/memory.save'.format(self.dest, domain.name())
        else:
            memory_path = None
        domain_disks = self.libvirt_client.get_domain_disks(domain)
        for index, disk in enumerate(domain_disks):
            if disk['path'] not in self.exclude_disks:
                domain_disks[index]['snapshot_path'] = '{0}/{1}/snapshot_{2}.qcow2'.format(self.snap, domain.name(), disk['target'])
        log.info('Create snapshot of domain {0}'.format(domain.name()))
        self.libvirt_client.create_vm_snapshot(domain, domain_disks, memory_path=memory_path)
        for disk in domain_disks:
            if disk['path'] not in self.exclude_disks:
                log.info('Create copy of {3} to {0}/{1}/{2}.img.gz'.format(self.dest, domain.name(),
                                                                           os.path.basename(disk['path']),
                                                                           disk['path']))
                ebt_files.dd.create(source=disk['path'], dest='{0}/{1}/{2}.img.gz'.format(self.dest, domain.name(),
                                                                                       os.path.basename(disk['path'])),
                                    compress_level=6)
                open('{0}/{1}/{2}.img.size'.format(self.dest, domain.name(), os.path.basename(disk['path'])), mode='w').write(
                    str(self.libvirt_client.device_size(domain, disk['target'])))
            log.info('Remove snapshot of domain {0}'.format(domain.name()))
            self.libvirt_client.remove_vm_snapshot(domain, domain_disks)