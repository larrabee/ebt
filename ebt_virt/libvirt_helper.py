import sys
import libvirt
from xml.etree import ElementTree
import time
import ebt_system


class Libvirt(object):
    def __init__(self, uri='qemu:///system'):
        self.conn = libvirt.open(uri)

    def list_domains(self):
        return self.conn.listAllDomains()

    @staticmethod
    def get_domain_disks(domain):
        assert isinstance(domain, libvirt.virDomain), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('domain', __name__, sys._getframe().f_code.co_name)
        domain_xml = domain.XMLDesc(0)
        root = ElementTree.fromstring(domain_xml)
        disks = root.findall('./devices/disk')
        disks_list = list()
        for disk in disks:
            disk_info = {}
            if disk.attrib['device'] in ('disk',):
                if (disk.find('source') is not None) and (disk.find('source').get('dev') is not None):
                    disk_info['path'] = disk.find('source').get('dev')
                    disk_info['target'] = disk.find('target').get('dev')
                    disk_info['source_type'] = 'dev'
                    disk_info['snapshot_path'] = None
                    disks_list.append(disk_info)
                elif (disk.find('source') is not None) and (disk.find('source').get('file') is not None):
                    disk_info['path'] = disk.find('source').get('file')
                    disk_info['target'] = disk.find('target').get('dev')
                    disk_info['source_type'] = 'file'
                    disk_info['snapshot_path'] = None
                    disks_list.append(disk_info)
        return disks_list

    def filter_domain_list(self, domains, include=list(), exclude=list()):
        assert isinstance(include, list), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('include', __name__, sys._getframe().f_code.co_name)
        assert isinstance(exclude, list), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('exclude', __name__, sys._getframe().f_code.co_name)
        assert isinstance(domains, list) and isinstance(domains[0],
                                                        libvirt.virDomain), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('domains', __name__, sys._getframe().f_code.co_name)
        filtered_list = list()
        for domain in domains:
            if (domain.name() in include) or (('all' not in exclude) and (domain.name() not in exclude)):
                filtered_list.append(domain)
        return filtered_list

    @staticmethod
    def export_xml(domain, path):
        domain_xml = domain.XMLDesc(0)
        xml_file = open(path, mode='w')
        xml_file.write(domain_xml)
        xml_file.close()

    @staticmethod
    def device_size(domain, path):
        return int(domain.blockInfo(path)[0])

    def restore(self, path):
        self.conn.restore(path)

    @staticmethod
    def create_snapshot_xml(disks, memory_path=None):
        assert isinstance(disks, list) and isinstance(disks[0], dict), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('disks', __name__, sys._getframe().f_code.co_name)
        assert (memory_path is None) or isinstance(memory_path, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('memory_path', __name__, sys._getframe().f_code.co_name)
        snap_xml = ElementTree.Element('domainsnapshot')
        disks_xml = ElementTree.SubElement(snap_xml, 'disks')
        if memory_path is None:
            ElementTree.SubElement(snap_xml, 'memory', {'snapshot': 'no'})
        else:
            ElementTree.SubElement(snap_xml, 'memory', {'snapshot': 'external', 'file': memory_path})
        for disk in disks:
            if disk['snapshot_path'] is None:
                ElementTree.SubElement(disks_xml, 'disk', {'name': disk['target'], 'snapshot': 'no'})
            else:
                disk_xml = ElementTree.SubElement(disks_xml, 'disk', {'name': disk['target'], 'snapshot': 'external'})
                ElementTree.SubElement(disk_xml, 'source', {'file': disk['snapshot_path']})
        snap_xml_str = ElementTree.tostring(snap_xml, encoding='utf8', method='xml')
        return snap_xml_str

    def create_vm_snapshot(self, domain, disks, memory_path=None, atomic=True, quiesce=False):
        assert isinstance(domain, libvirt.virDomain), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('domain', __name__, sys._getframe().f_code.co_name)
        assert isinstance(disks, list) and isinstance(disks[0], dict), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('disks', __name__, sys._getframe().f_code.co_name)
        assert (memory_path is None) or isinstance(memory_path, str), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('memory_path', __name__, sys._getframe().f_code.co_name)
        assert isinstance(atomic, bool), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('atomic', __name__, sys._getframe().f_code.co_name)
        assert isinstance(quiesce, bool), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('quiesce', __name__, sys._getframe().f_code.co_name)
        flags = 0
        if memory_path is None:
            flags |= libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_DISK_ONLY
        else:
            flags |= libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_LIVE
        if atomic:
            flags |= libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_ATOMIC
        if quiesce:
            flags |= libvirt.VIR_DOMAIN_SNAPSHOT_CREATE_QUIESCE
        snap_xml = self.create_snapshot_xml(disks, memory_path)
        snap = domain.snapshotCreateXML(snap_xml, flags)
        return snap

    @staticmethod
    def remove_vm_snapshot(domain, disks):
        assert isinstance(domain, libvirt.virDomain), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('domain', __name__, sys._getframe().f_code.co_name)
        assert isinstance(disks, list) and isinstance(disks[0], dict), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('disks', __name__, sys._getframe().f_code.co_name)
        flags = libvirt.VIR_DOMAIN_BLOCK_COMMIT_ACTIVE
        for disk in disks:
            if disk['snapshot_path'] is not None:
                domain.blockCommit(disk=disk['target'], base=None, top=None, flags=flags)
                while True:
                    status = domain.blockJobInfo(disk['target'])
                    if status['cur'] == status['end']:
                        domain.blockJobAbort(disk=disk['target'], flags=libvirt.VIR_DOMAIN_BLOCK_JOB_ABORT_PIVOT)
                        ebt_system.rm(disk['snapshot_path'])
                        break
                    else:
                        time.sleep(3)

