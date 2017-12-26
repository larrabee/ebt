import logging
import MySQLdb
from ebt_system import popen as _popen
from multiprocessing import cpu_count


class Mysql:
    def __init__(self, params):
        self.log = logging.getLogger('__main__')
        self.params = params

    def exec_command(self, sql_command):
        if 'unix_socket' in self.params:
            if self.params['passwd'] is None:
                db = MySQLdb.connect(unix_socket=self.params['unix_socket'], user=self.params['user'], charset='utf8')
            else:
                db = MySQLdb.connect(unix_socket=self.params['unix_socket'], user=self.params['user'],
                                     passwd=self.params['passwd'],
                                     charset='utf8')
        else:
            if self.params['passwd'] is None:
                db = MySQLdb.connect(host=self.params['host'], port=self.params['port'], user=self.params['user'],
                                     charset='utf8')
            else:
                db = MySQLdb.connect(host=self.params['host'], port=self.params['port'], user=self.params['user'],
                                     passwd=self.params['passwd'], charset='utf8')
        cursor = db.cursor(MySQLdb.cursors.DictCursor)
        self.log.debug('Mysql exec command: {0}'.format(sql_command))
        cursor.execute(sql_command)
        data = cursor.fetchall()
        db.commit()
        return data

    def slave_start(self):
        self.exec_command('start slave')

    def slave_stop(self):
        self.exec_command('stop slave')

    def slave_status(self):
        return self.exec_command('SHOW SLAVE STATUS')[0]

    def slave_status_to_file(self, file_name='slave_data'):
        slave_data_file = open('{0}/{1}'.format(self.params['dest'], file_name), 'w')
        slave_status = self.slave_status()
        slave_data_file.write(
            'CHANGE MASTER TO MASTER_HOST="{0}", MASTER_PORT={1}, MASTER_USER="{2}", MASTER_LOG_FILE="{3}", MASTER_LOG_POS={4};\n'.format(
                slave_status['Master_Host'], slave_status['Master_Port'], slave_status['Master_User'],
                slave_status['Master_Log_File'], slave_status['Exec_Master_Log_Pos']))
        for key, value in slave_status.iteritems():
            slave_data_file.write('#\t{0}: {1}\n'.format(key, value))
        slave_data_file.close()

    def mysqldump(self):
        for database in self.params['db']:
            command = 'mysqldump'
            if 'unix_socket' in self.params:
                command += ' -S{0}'.format(self.params['unix_socket'])
            else:
                command += ' -h{0} -P{1}'.format(self.params['host'], self.params['port'])
            if self.params['passwd'] is not None:
                command += ' -p{0}'.format(self.params['passwd'])
            command += ' -u{0} {1} {2}'.format(self.params['user'], self.params['dump_args'], database)
            if self.params['compress_level'] > 0:
                compress_threads = self.params['compress-threads'] if (
                    'compress-threads' in self.params) else cpu_count()
                command += ' |pigz -c -{2} -p {3} > {0}/{1}.sql.gz'.format(self.params['dest'], database,
                                                                           self.params['compress_level'],
                                                                           compress_threads)
            else:
                command += ' > {0}/{1}.sql'.format(self.params['dest'], database)
            self.log.debug('Mysql dump command: {0}'.format(command))
            _popen(command=command, shell=True)

    def innobackupex(self):
        command = 'innobackupex --no-timestamp'
        if 'unix_socket' in self.params:
            command += ' -S{0}'.format(self.params['unix_socket'])
        else:
            command += ' -h{0} -P{1}'.format(self.params['host'], self.params['port'])
        if self.params['passwd'] is not None:
            command += ' -p{0}'.format(self.params['passwd'])
        command += ' -u{0} {1}'.format(self.params['user'], self.params['dump_args'])
        if self.params['compress']:
            command += ' --compress --compress-threads={0}'.format(self.params['compress-threads'])
        if self.params['db']:
            command += ' --databases {0}'.format(' '.join(self.params['db']))
        command += ' {0}'.format(self.params['dest'])
        self.log.debug('Innobackupex command: {0}'.format(command))
        _popen(command=command, shell=True)
