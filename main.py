__author__ = 'larrabee'
#Cli
import argparse
#Logging
import logging
import logging.handlers
#ConfigObj
from configobj import ConfigObj
from validate import Validator
#System modules
import sys
import yaml

#Base vars
config_filename = '/home/larrabee/PycharmProjects/ebbt/ebbt.conf'
config_spec_filename = '/home/larrabee/PycharmProjects/ebbt/ebbt.spec'
module_dir = './modules'
formater = logging.Formatter(fmt="%(asctime)s %(levelname)s: %(message)s", datefmt="%d-%m-%Y %H:%M:%S")

#Import special modules
sys.path.append('.')
import modules.btrfs

#Comman line parser
cli_parser = argparse.ArgumentParser()
cli_parser.add_argument('-j', '--jobs', nargs='+', help='List of jobs to run')
cli_parser.add_argument('-c', '--cleanup', nargs='+', help='List of jobs to cleanup')
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
            smtp_handler = logging.handlers.SMTPHandler(fromaddr=mail_config['from'],
                                                        mailhost=(mail_config['server'], mail_config['port']),
                                                        toaddrs=mail_config['recipients'],
                                                        subject=mail_config['subject'],
                                                        credentials=(mail_config['login'], mail_config['password']),
                                                        secure=tuple())
        else:
            smtp_handler = logging.handlers.SMTPHandler(fromaddr=mail_config['from'],
                                                        mailhost=(mail_config['server'], mail_config['port']),
                                                        toaddrs=mail_config['recipients'],
                                                        subject=mail_config['subject'],
                                                        credentials=(mail_config['login'], mail_config['password']))
        smtp_handler.setFormatter(formater)
        memory_handler = logging.handlers.MemoryHandler(capacity=10240*1000, target=smtp_handler)
        #log.addHandler(memory_handler)
except KeyError:
    log.error('"Mail" section not found in configuration file')
else:
    log.debug('SMTP handler successfully added')


stream = open('job.yml', 'r')
job_config = yaml.load(stream)
print(job_config)
"""
for job in cli.jobs:
    log.info('Job "{0}" started'.format(job))
    try:
        job_config = cfg_parser['jobs'][job]
    except KeyError:
        log.error('"{0}" section not found in configuration file'.format(job))
        log.error('Job "{0}" failed'.format(job))
        continue
    else:
        log.debug('Job "{0}" configuration successfully read'.format(job))
    try:
        module = __import__(job_config['type'])
    except ImportError:
        log.error('Cannot import module "{0}"'.format(job_config['type']))
        log.error('Job "{0}" failed'.format(job))
        continue
    else:
        log.debug('Successful import module "{0}"'.format(job_config['type']))
    try:
        module.run(cfg=cfg_parser, job_config=job_config, log=log)
    except:
        log.error('Job execution failed with error: {0}'.format(sys.exc_info()))
"""

