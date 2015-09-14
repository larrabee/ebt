#!/usr/bin/python3
__author__ = 'larrabee'
import subprocess



def __popen(command):
    process = subprocess.Popen(command.split(), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    output = process.communicate()[0]
    if process.returncode != 0:
        raise RuntimeError('Error code:', str(process.returncode), 'Output: ', output, 'Command: ', command)
    return output

def sub_list(path):
    command = 'btrfs subvolume list {0}'.format(str(path))
    raw_output = __popen(command)
    form_output = raw_output.decode()
    output = list()
    for string in form_output.split(sep='\n'):
        string = string.split(sep=' ')
        if len(string) > 1:
            output_element = {'id': string[1], 'gen': string[3], 'parrent': string[6], 'path': string[8]}
            output.append(output_element)
    return output

def sub_del(path):
    command = 'btrfs subvolume delete {0}'.format(str(path))
    __popen(command)

def sub_snap(path, snap_path, readonly=True):
    command = 'btrfs subvolume snapshot {0} {1}'.format(str(path), str(snap_path))
    if readonly:
        command += ' -r'
    __popen(command)

def send(path, dest_path, parrent_path=None):
    command = 'btrfs send {0} -f {1}'.format(str(path), str(dest_path))
    if parrent_path is not None:
        command += ' -p {0}'.format(str(parrent_path))
    __popen(command)

def sub_getdef(path):
    command = 'btrfs subvolume get-default {0}'.format(str(path))
    raw_output = __popen(command)
    form_output = raw_output.decode()
    string = int(form_output.split(sep='\n')[0].split(sep=' ')[1])
    return string


