__author__ = 'larrabee'
import logging
import sys
import MySQLdb
import modules.sys_mod


class Mysql:
    def __init__(self):
        self.log = logging.getLogger('__main__')

    def connect_to_tcp(self, user, passwd, host='localhost', port=3306):
        self.con = MySQLdb.connect(host=host, user=user, passwd=passwd, port=port)
        self.cur = self.con.cursor(MySQLdb.cursors.DictCursor)

    def connect_to_unix(self, user, passwd, unix_socket='/var/lib/mysql/mysql.sock'):
        self.con = MySQLdb.connect(unix_socket=unix_socket, user=user, passwd=passwd)
        self.cur = self.con.cursor(MySQLdb.cursors.DictCursor)

    def slave_status(self):
        self.cur.execute('show slave status')
        slave_status = self.cur.fetchone()
        slave_sql_running = 1 if slave_status["Slave_SQL_Running"] == "Yes" else 0
        slave_io_running = 1 if slave_status["Slave_IO_Running"] == "Yes" else 0
        return slave_io_running + slave_sql_running

    def slave_stop(self):
        self.cur.execute('stop slave')
        return self.slave_status()

    def slave_start(self):
        self.cur.execute('start slave')
        return self.slave_status()

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
                command += ' |gzip -c -{0} > {1}/{2}.sql.gz'.format(params['compress_level'], params['dest'], database)
            else:
                command += ' > {0}/{1}.sql'.format(params['dest'], database)
            self.log.debug('Mysql dump command: {0}'.format(command))
            modules.sys_mod.Sys().popen(command=command, shell=True)
