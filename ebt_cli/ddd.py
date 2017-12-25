# -*- coding: utf-8 -*-
from ebt_files import ddd


def ddd_cli():
    import sys
    import argparse

    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('-i', '--if', type=str, default=sys.stdin, help='Input File (Full copy)')
    cli_parser.add_argument('-d', '--df', type=str, default=sys.stdin,
                            help='Diff File. For backup mode: changed file. For restore mode: binary diff file')
    cli_parser.add_argument('-o', '--of', type=str, default=sys.stdout,
                            help='Output File. For backup mode: binary diff. For restore mode: restored file')
    cli_parser.add_argument('-m', '--mode', choices=['backup', 'restore'], default='backup', type=str, required=False,
                            help='Working mode')
    cli_parser.add_argument('-b', '--block-size', type=int, default=8192, help='Block size')
    cli = cli_parser.parse_args()

    if (vars(cli)['if'] == sys.stdin) and (vars(cli)['df'] == sys.stdin) and (sys.stdin.isatty() is True):
        sys.stderr.write("--if and --df files should be specified\n")
        exit(1)
    elif (vars(cli)['if'] == sys.stdin) and (vars(cli)['df'] == sys.stdin) and (sys.stdin.isatty() is False):
        sys.stderr.write("Can not read both (--if and --df) files from stdin.\nYou should specify at least one\n")
        exit(1)

    if vars(cli)['if'] == sys.stdin:
        iffd = sys.stdin
    else:
        iffd = open(vars(cli)['if'], 'rb')
    if vars(cli)['df'] == sys.stdin:
        dffd = sys.stdin
    else:
        dffd = open(vars(cli)['df'], 'rb')
    if vars(cli)['of'] == sys.stdout:
        offd = sys.stdout
    else:
        offd = open(vars(cli)['of'], 'wb')

    if cli.mode == 'backup':
        differ = ddd.CreateDiff(iffd=iffd, dffd=dffd, offd=offd, block_size=cli.block_size)
        differ.start()
    elif cli.mode == 'restore':
        differ_restore = ddd.RestoreDiff(iffd=iffd, dffd=dffd, offd=offd, block_size=cli.block_size)
        differ_restore.start()
