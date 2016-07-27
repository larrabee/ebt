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
        cursor = db.cursor()
        self.log.debug('Mysql exec command: {0}'.format(sql_command))
        cursor.execute(sql_command)
        data =  cursor.fetchall()
        db.commit()
        return data

    def slave_start(self, params):
        self.exec_command(params=params, sql_command='start slave;')

    def slave_stop(self, params):
        self.exec_command(params=params, sql_command='stop slave;')

    def slave_status(self, params):
        return self.exec_command(params=params, sql_command='SHOW SLAVE STATUS\G')[0]

    def mysqldump(self, params):
        assert isinstance(params, dict), '{1}.{2}: variable "{0}" has wrong type.' \
            .format('params', __name__, sys._getframe().f_code.co_name)
        assert ('user' in params) and isinstance(params['user'], str)
        assert ('passwd' in params) and (isinstance(params['passwd'], str) or isinstance(params['passwd'], None))
        assert ('db' in params) and isinstance(params['db'], list) and isinstance(params['db'][0], str)
        assert ('dump_args' in params) and isinstance(params['dump_args'], str)
        assert ('compress_level' in params) and (params['compress_level'] in range(0, 9))
        assert ('dest' in params) and isinstance(params['dest'], str)
        assert (('host' in params) and ('port' in params) and isinstance(params['host'], str) and isinstance(
            params['port'], int)) or (('unix_socket' in params) and isinstance(params['unix_socket'], str))

        #open('{0}/slave_data'.format(params['dest']), 'w').write(str(self.slave_status(params)))
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
                command += ' |pbzip2 -c > {0}/{1}.sql.pbzip2'.format(params['dest'], database)
            else:
                command += ' > {0}/{1}.sql'.format(params['dest'], database)
            self.log.debug('Mysql dump command: {0}'.format(command))
            modules.sys_mod.Sys().popen(command=command, shell=True)
