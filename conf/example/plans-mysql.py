#!/usr/bin/python
__author__ = 'larrabee'
import modules.sys_mod
import logging
import datetime
import modules.databases
import os

log = logging.getLogger('__main__')

def mysql():
    backup_date = datetime.datetime.now().strftime('%d-%m-%Y_%H:%M')
    instances = [{'host': 'db.lan', 'port': 4302, 'user': 'root', 'passwd': '123', 'db': ['test', ],
                  'dump_args': '--routines --triggers --dump-slave --include-master-host-port',
                  'dest': '/mnt/tmp/', 'compress_level': 3},
                 ]

    for instance in instances:
        try:
            log.info('Cleanup Dest dir: {0}'.format(instance['dest']))
            modules.sys_mod.Sys().rm(instance['dest'])
        except Exception:
            pass
    for instance in instances:
        mysql_client = modules.databases.Mysql()
        mysql_client.connect_to_tcp(user=instance['user'], passwd=instance['passwd'], host=instance['host'],
                                    port=instance['port'])
        instance['dest'] += '/{0}'.format(backup_date)
        log.info('Create dest dir: {0}'.format(instance['dest']))
        os.makedirs(instance['dest'])
        log.info('Start backup databases {0} from {1}:{2}'.format(instance['db'], instance['host'], instance['port']))
        log.info('Stop slave on server {0}:{1}'.format(instance['host'], instance['port']))
        mysql_client.slave_stop()
        if mysql_client.slave_status() == 2:
            log.error('Slave not stoped on server {0}:{1}'.format(instance['host'], instance['port']))
            raise UserWarning
        mysql_client.mysqldump(instance)
        log.info('Backup databases {0} from {1}:{2} finished'.format(instance['db'], instance['host'], instance['port']))
    log.info('Start slave on server {0}:{1}'.format(instance['host'], instance['port']))
    mysql_client.slave_start()
