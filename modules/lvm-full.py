__author__ = 'larrabee'
import os
import re
import subprocess
from _datetime import datetime

os.makedirs('/tmp/test34/test1')

snapshot_size = '10G'

def exec(command):
    process = subprocess.Popen(command.split(), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    output = str(process.communicate())
    if process.returncode != 0:
        raise ExecutionError('Error code:', str(process.returncode), 'Output: ', output)
    return output


def lvm_backup_check_vars():
    try:
        sopathmatch = re.match(r'(/dev/)(([\w\.*\-*])+/)+', job.sopath)
        if sopathmatch.group() != job.sopath:
            raise ValueError()
    except ValueError or TypeError:
        log.add('Invalid source path: ', job.sopath, traceback=traceback.format_exc(), mtype=3)
    except:
        log.add('Invalid path or another unknown error: ', job.sopath, traceback=traceback.format_exc(), mtype=3)
    else:
        log.add('lvm_backup_check_vars: source path is good')
    try:
        dpathmatch = re.match(r'/([\w*\d*\-*\.*,*\\\\*]+/)*', job.dpath)
        if dpathmatch.group() != job.dpath:
            raise ValueError()
    except ValueError or TypeError:
        log.add('Invalid destination path: ', job.dpath, traceback=traceback.format_exc(), mtype=3)
    except:
        log.add('Invalid path or another unknown error: ', job.dpath, traceback=traceback.format_exc(), mtype=3)
    else:
        log.add('lvm_backup_check_vars: destination path is good')


def lvm_check_exist_snapshot():
    lvmlistsopath = os.listdir(job.sopath)
    for element in lvmlistsopath:
        if conf.temp_snap_name in element:
            try:
                command = ['lvremove', '--autobackup', 'y', '--force', job.sopath + element]
                return_code = exec_shell_command(command=command)
                if return_code != 0:
                    raise SystemError
            except:
                log.add('Cannot delete: ', job.sopath + element, mtype=3)
            else:
                log.add('Delete old snapshot: ', job.sopath + element)


def lvm_generate_backup_list():
    lvmbackuplist = []
    lvmlistsopath = os.listdir(job.sopath)
    for element in lvmlistsopath:
        if (element in job.include) or (job.exclude != 'all') and (element not in job.exclude):
            lvmbackuplist.append(element)
    if not lvmbackuplist:
        log.add('Null backup list, exiting', mtype=3)
    return lvmbackuplist


def lvm_create_snapshot(lvmsnapsource=None):
    try:
        command = ['lvcreate', '--size', '10G', '-A', 'y', '--snapshot', '--name', conf.temp_snap_name, lvmsnapsource]
        return_code = exec_shell_command(command=command)
        if return_code != 0:
            raise SystemError
    except SystemError:
        log.add('Cannot create snapshot of ', lvmsnapsource, mtype=3)
    except:
        log.add('Unknown error while creating snapshot of ', lvmsnapsource, mtype=3)
    else:
        log.add('Successfully create snapshot of: ', lvmsnapsource)


def create_copy_with_dd(ddif=None, ddof=None):
    try:
        command = ['dd', 'if=' + ddif, 'of=' + ddof, 'bs=' + conf.bs]
        return_code = exec_shell_command(command=command)
        if return_code != 0:
            raise SystemError
    except:
        log.add('Cannot create dd copy. IF: ', ddif, ' OF: ', ddof, mtype=2)
    else:
        log.add('Successfully create dd copy of: ', ddif, ' to: ', ddof)


def run(cfg, job_config, log, **kwargs):
    pass