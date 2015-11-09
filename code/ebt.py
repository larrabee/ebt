#!/usr/bin/python3
__author__ = 'larrabee'
#Cli
import argparse
#Logging
import logging
import logging.handlers
import sys
import traceback
#ConfigObj
from configobj import ConfigObj
from validate import Validator
#Other
import inspect
import os
#My modules
import modules.logging
conf_dir = '/etc/ebt'
sys.path.append(conf_dir)
import plans

#Base vars
version = '0.65'
config_filename = conf_dir + '/ebt.conf'
config_spec_filename = os.path.dirname(os.path.realpath(__file__)) + '/ebt.spec'
formater = logging.Formatter(fmt="%(asctime)s %(levelname)s: %(message)s", datefmt="%d-%m-%Y %H:%M:%S")

#Comman line parser
cli_parser = argparse.ArgumentParser()
cli_parser.add_argument('-j', '--jobs', nargs='+', help='List of jobs to run')
cli_parser.add_argument('-v', '--version', default=False, action='store_true', help='Display program version and exit')
cli = cli_parser.parse_args()
#Base logging
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d-%m-%Y %H:%M:%S", level=logging.DEBUG)
log = logging.getLogger(__name__)

#Config Parser
cfg_parser = ConfigObj(config_filename, configspec=config_spec_filename)
validator = Validator()
result = cfg_parser.validate(validator)
if result is not True:
    log.critical('Config validation failed.')
    log.debug('Validation result: {0}'.format(result))
    exit(1)


try:
    config = cfg_parser['Config']
except KeyError:
    log.critical('"Config" section not found in configuration file: "{0}" or file not found'.format(config_filename))
    exit(1)

#Logging
if config['loglevel'] == 'debug':
    log.setLevel(logging.DEBUG)
elif config['loglevel'] == 'info':
    log.setLevel(logging.INFO)
elif config['loglevel'] == 'warn':
    log.setLevel(logging.WARN)
elif config['loglevel'] == 'error':
    log.setLevel(logging.ERROR)
elif config['loglevel'] == 'crit':
    log.setLevel(logging.CRITICAL)



try:
    if 'syslog' in config['logmethod']:
        syslog_handler = logging.handlers.SysLogHandler(address='/dev/log',  facility='user')
        log.addHandler(syslog_handler)
        log.debug('Add syslog handler to logger')
except KeyError:
    pass
else:
    log.debug('Syslog handler successfully added')
try:
    if 'file' in config['logmethod']:
        file_handler = logging.handlers.RotatingFileHandler(config['logfile'], maxBytes=config['max_log_size'])
        file_handler.setFormatter(formater)
        log.addHandler(file_handler)
        log.debug('Add file handler to logger')
except KeyError:
    pass
except FileNotFoundError:
    log.error('No such directory: {0}. File handler not added to logger.'.format(config['logfile']))
except PermissionError:
    log.error('Permission denied: {0}. File handler not added to logger.'.format(config['logfile']))
else:
    log.debug('File handler successfully added')

try:
    mail_config = cfg_parser['MailConfig']
    if 'smtp' in config['logmethod']:
        if mail_config['tls']:
            secure = tuple()
        else:
            secure = None

        smtp_handler = modules.logging.BufferingSMTPHandler(fromaddr=mail_config['from'], mailhost=(mail_config['server'],
                                                    mail_config['port']), toaddrs=mail_config['recipients'],
                                                    subject=mail_config['subject'], secure=secure,
                                                    credentials=(mail_config['login'], mail_config['password']))
        smtp_handler.setFormatter(formater)
        log.addHandler(smtp_handler)
except KeyError:
    log.error('"Mail" section not found in configuration file')
else:
    log.debug('SMTP handler successfully added')


def getfunctions(function):
    functions_raw = inspect.getmembers(function, inspect.isfunction)
    functions = list()
    for item in functions_raw:
        functions.append(item[0])
    return functions

if cli.version:
    print('Version: ' + version)
    exit(0)

if cli.jobs is None:
    print('Nothing to do')
    exit(0)

log.info('=' * 30 + 'Program started' + '=' * 30)
for job in cli.jobs:
    if job not in getfunctions(plans):
        log.error('Job "{0}" not found in plans.py'.format(job))
        break
    log.info('-' * 30 + 'Job "{0}" started'.format(job) + '-' * 30)
    try:
        plan = 'plans.' + job + '()'
        exec(plan)
    except AssertionError as e:
        log.error('Assertion Error')
        log.debug(e)
    except Exception as e:
        log.error(e)
        log.debug(traceback.format_exc())
    log.info('-' * 30 + 'Job "{0}" finished'.format(job) + '-' * 30)
log.info('=' * 30 + 'Program halted' + '=' * 30)

