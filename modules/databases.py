__author__ = 'larrabee'
import logging
import sys
import MySQLdb
import modules.sys_mod


class Mysql:
    def __init__(self):
        self.log = logging.getLogger('__main__')

    def exec_command(self, params, sql_command):
        if 'unix_socket' in params:
            if params['passwd'] is None:
                db = MySQLdb.connect(unix_socket=params['unix_socket'], user=params['user'], charset='utf8')
            else:
                db = MySQLdb.connect(unix_socket=params['unix_socket'], user=params['user'], passwd=params['passwd'], charset='utf8')
        else:
            if params['passwd'] is None:
                db = MySQLdb.connect(host=params['host'], port=params['port'], user=params['user'], charset='utf8')
            else:
                db = MySQLdb.connect(host=params['host'], port=params['port'], user=params['user'], passwd=params['passwd'], charset='utf8')
        cursor = db.cursor(MySQLdb.cursors.DictCursor)
        self.log.debug('Mysql exec command: {0}'.format(sql_command))
        cursor.execute(sql_command)
        data =  cursor.fetchall()
        db.commit()
        return data

    def slave_start(self, params):
        self.exec_command(params=params, sql_command='start slave')

    def slave_stop(self, params):
        self.exec_command(params=params, sql_command='stop slave')

    def slave_status(self, params):
        return self.exec_command(params=params, sql_command='SHOW SLAVE STATUS')[0]

    def slave_status_to_file(self, params, file_name='slave_data'):
        slave_data_file = open('{0}/{1}'.format(params['dest'], file_name), 'w')
        slave_status = self.slave_status(params)
        slave_data_file.write('CHANGE MASTER TO MASTER_HOST="{0}", MASTER_PORT={1}, MASTER_USER="{2}", MASTER_LOG_FILE="{3}", MASTER_LOG_POS={4};\n' \
                              .format(slave_status['Master_Host'], slave_status['Master_Port'], slave_status['Master_User'], slave_status['Master_Log_File'], slave_status['Exec_Master_Log_Pos']))
        for key, value in slave_status.iteritems():
            slave_data_file.write('#\t{0}: {1}\n'.format(key, value))
        slave_data_file.close()

    def mysqldump(self, params):
        assert isinstance(params, dict), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('params', __name__, sys._getframe().f_code.co_name)
        assert ('user' in params) and isinstance(params['user'], str)
        assert ('passwd' in params) and (isinstance(params['passwd'], str) or (params['passwd'] is None))
        assert ('db' in params) and isinstance(params['db'], list) and isinstance(params['db'][0], str)
        assert ('dump_args' in params) and isinstance(params['dump_args'], str)
        assert ('compress_level' in params) and (params['compress_level'] in range(0, 9))
        assert ('dest' in params) and isinstance(params['dest'], str)
        assert (('host' in params) and ('port' in params) and isinstance(params['host'], str) and isinstance(
            params['port'], int)) or (('unix_socket' in params) and isinstance(params['unix_socket'], str))

        for database in params['db']:
            command = 'mysqldump'
            if 'unix_socket' in params:
                command += ' -S{0}'.format(params['unix_socket'])
            else:
                command += ' -h{0} -P{1}'.format(params['host'], params['port'])
            if params['passwd'] is not None:
                command += ' -p{0}'.format(params['passwd'])
            command += ' -u{0} {1} {2}'.format(params['user'], params['dump_args'], database)
            if params['compress_level'] > 0:
                command += ' |pigz -c -{2} > {0}/{1}.sql.gz'.format(params['dest'], database, params['compress_level'])
            else:
                command += ' > {0}/{1}.sql'.format(params['dest'], database)
            self.log.debug('Mysql dump command: {0}'.format(command))
            modules.sys_mod.Sys().popen(command=command, shell=True)
            
    def innobackupex(self, params):
        assert isinstance(params, dict), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('params', __name__, sys._getframe().f_code.co_name)
        assert ('user' in params) and isinstance(params['user'], str)
        assert ('passwd' in params) and (isinstance(params['passwd'], str) or isinstance(params['passwd'], None))
        assert ('db' in params) and (isinstance(params['db'], list) or (params['db'] is None))
        assert ('dump_args' in params) and isinstance(params['dump_args'], str)
        assert ('compress' in params) and isinstance(params['compress'], bool)
        if params['compress']:
            assert ('compress-threads' in params) and (params['compress-threads'] in range(1, 99))
        assert ('dest' in params) and isinstance(params['dest'], str)
        assert (('host' in params) and ('port' in params) and isinstance(params['host'], str) and isinstance(
            params['port'], int)) or (('unix_socket' in params) and isinstance(params['unix_socket'], str))
        
        command = 'innobackupex'
        if 'unix_socket' in params:
            command += ' -S{0}'.format(params['unix_socket'])
        else:
            command += ' -h{0} -P{1}'.format(params['host'], params['port'])
        if params['passwd'] is not None:
            command += ' -p{0}'.format(params['passwd'])
        command += ' -u{0} {1}'.format(params['user'], params['dump_args'])
        if params['compress']:
            command += ' --compress --compress-threads={0}'.format(params['compress-threads'])
        if params['db']:
            command += ' --databases {0}'.format(' '.join(params['db']))
        command += ' {0}'.format(params['dest'])
        self.log.debug('Innobackupex command: {0}'.format(command))
        modules.sys_mod.Sys().popen(command=command, shell=True)
