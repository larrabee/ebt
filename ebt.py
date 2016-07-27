#!/usr/bin/python
__author__ = 'larrabee'
# Cli
import argparse
# Logging
import sys
import traceback
# ConfigObj
from configobj import ConfigObj
from validate import Validator
# Other
import os
# My modules
import modules.custom_logging
import modules.sys_mod

# Base vars
version = '1.1'
config_spec_filename = os.path.dirname(os.path.realpath(__file__)) + '/ebt.spec'

# Command line parser
cli_parser = argparse.ArgumentParser()
cli_parser.add_argument('-j', '--jobs', nargs='+', help='List of jobs to run')
cli_parser.add_argument('-p', '--plan', default='/etc/ebt/plans.py', type=str, help='Custom path to plans file')
cli_parser.add_argument('-c', '--config', default='/etc/ebt/ebt.conf', type=str, help='Custom path to config file')
cli_parser.add_argument('-v', '--version', default=False, action='store_true', help='Display program version and exit')
cli = cli_parser.parse_args()

#Import plans file
sys.path.append(os.path.dirname(cli.plan))
plans = __import__(os.path.splitext(os.path.basename(cli.plan))[0])

# Base logging
log_configurator = modules.custom_logging.Configurator()
log = log_configurator.get_logger()

# Config Parser
cfg_parser = ConfigObj(cli.config, configspec=config_spec_filename)
validator = Validator()
result = cfg_parser.validate(validator)
if result is not True:
    log.critical('Config validation failed.')
    log.debug('Validation result: {0}'.format(result))
    exit(1)

try:
    config = cfg_parser['Config']
except KeyError:
    log.critical('"Config" section not found in configuration file: "{0}"'.format(cli.config))
    exit(1)

if cli.version:
    print('Version: ' + version)
    exit(0)

if cli.jobs is None:
    print('Nothing to do')
    exit(0)

# Logging
log_configurator.set_level(severity=config['loglevel'])

if 'file' in config['logmethod']:
    try:
        log_configurator.add_file_handler(path=config['logfile'], max_size=config['max_log_size'])
    except Exception:
        log.error('Cannot add file handler')

if 'syslog' in config['logmethod']:
    try:
        log_configurator.add_syslog_handler()
    except Exception:
        log.error('Cannot add syslog handler')

if 'smtp' in config['logmethod']:
    try:
        mail_config = cfg_parser['MailConfig']
        if mail_config['tls']:
            secure = tuple()
        else:
            secure = None
        log_configurator.add_smtp_handler(fromaddr=mail_config['from'],
                                          mailhost=(mail_config['server'], mail_config['port']),
                                          toaddrs=mail_config['recipients'], subject=mail_config['subject'],
                                          credentials=(mail_config['login'], mail_config['password']), secure=secure)
    except Exception:
        log.error('Cannot add SMTP handler')


log.info('=' * 30 + 'Program started' + '=' * 30)
exit_code = 0
for job in cli.jobs:
    if job not in modules.sys_mod.Sys().getfunctions(plans):
        log.error('Job "{0}" not found in plans-libvirt.py'.format(job))
        break
    log.info('-' * 30 + 'Job "{0}" started'.format(job) + '-' * 30)
    try:
        plan = 'plans.' + job + '()'
        exec(plan)
    except AssertionError as e:
        log.error('Assertion Error: {0}'.format(e))
        _, _, tb = sys.exc_info()
        log.debug(traceback.format_tb(tb))
        exit_code = 255
    except Exception as e:
        log.error(e)
        log.debug(traceback.format_exc())
        exit_code =255
    log.info('-' * 30 + 'Job "{0}" finished'.format(job) + '-' * 30)
log.info('=' * 30 + 'Program halted' + '=' * 30)
exit(exit_code)
