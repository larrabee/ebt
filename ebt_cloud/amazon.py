import logging
from boto.glacier.layer2 import Layer2
from boto import connect_s3
import boto.s3.connection
import time
import datetime
import ebt_system
import os
import multiprocess as mp
from functools import partial
from contextlib import contextmanager


class AmazonGlacier:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.log = logging.getLogger('__main__')
        logging.getLogger('boto').setLevel(logging.CRITICAL)
        self.glacier_client = Layer2(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                                     region_name=region_name)

    def upload_file(self, vault_name, description, path):
        if self.is_vault_exist(vault_name):
            vault = self.glacier_client.get_vault(vault_name)
        else:
            vault = self.create_vault(vault_name)
        archive_id = vault.concurrent_create_archive_from_file(path, description, part_size=4194304)
        self.log.debug('File "{0}" upload successful. Archive id: "{1}"'.format(path, archive_id))
        return archive_id

    def create_vault(self, vault_name):
        vault = self.glacier_client.create_vault(name=vault_name)
        return vault

    def is_vault_exist(self, vault_name):
        vaults = [i.name for i in self.glacier_client.list_vaults()]
        if vault_name in vaults:
            return True
        else:
            return False

    def get_inventory(self, vault_name, sleep_interval=1200):
        vault = self.glacier_client.get_vault(vault_name)
        inventory_job_id = vault.retrieve_inventory()
        job = vault.get_job(inventory_job_id)
        while not job.completed:
            time.sleep(sleep_interval)
            job = vault.get_job(inventory_job_id)
        inventory = job.get_output()
        return inventory

    def download_file(self, vault_name, archive_id, dest, sleep_interval=1200):
        vault = self.glacier_client.get_vault(vault_name)
        job = vault.retrieve_archive(archive_id)
        job_id = job.id
        while not job.completed:
            time.sleep(sleep_interval)
            job = vault.get_job(job_id)
        download_result = job.download_to_file(dest)
        return download_result

    def delete_archive(self, vault_name, archive_id):
        vault = self.glacier_client.get_vault(vault_name)
        vault.delete_archive(archive_id)
        self.log.debug('Successfully remove archive "{0}" from vault "{1}"'.format(archive_id, vault.name))


class S3(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, **kwargs):
        self.s3_client = connect_s3(aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                    calling_format=boto.s3.connection.OrdinaryCallingFormat(), **kwargs)
        logging.getLogger('boto').setLevel(logging.CRITICAL)

    @staticmethod
    def _is_dir(path):
        if path == "{0}/".format(os.path.dirname(path)):
            return True
        else:
            return False

    def _get_bucket_by_name(self, bucket_name):
        return self.s3_client.get_bucket(bucket_name)

    def list_bucket(self, bucket_name, max_keys):
        bucket = self._get_bucket_by_name(bucket_name)
        more_results = True
        k = None
        marker = ''
        while more_results:
            rs = bucket.get_all_keys(marker=marker, max_keys=max_keys)
            for k in rs:
                k.last_modified_dt = datetime.datetime.strptime(k.last_modified, '%Y-%m-%dT%H:%M:%S.%fZ') - (datetime.datetime.utcnow() - datetime.datetime.now())
                yield k
            if k:
                marker = rs.next_marker or k.name
            more_results = rs.is_truncated

    @staticmethod
    @contextmanager
    def poolcontext(*args, **kwargs):
        pool = mp.Pool(*args, **kwargs)
        yield pool
        pool.terminate()

    @staticmethod
    def _dump_file(file, dest_dir):
        dest = os.path.join(dest_dir, file.name)
        ebt_system.makedirs(os.path.dirname(dest), exist_ok=True)
        file.get_contents_to_filename(dest)

    def dump_files(self, files, dest_dir, workers=48):
        with self.poolcontext(processes=workers) as pool:
            pool.map(partial(self._dump_file, dest_dir=dest_dir), files)
