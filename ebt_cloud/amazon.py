import logging
from boto.glacier.layer2 import Layer2
import time


class Amazon:
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
        archive_id = self.__upload_file(vault=vault, description=description, path=path)
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

    def __upload_file(self, vault, description, path):
        archive_id = vault.concurrent_create_archive_from_file(path, description, part_size=4194304)
        self.log.debug('File "{0}" upload successful. Archive id: "{1}"'.format(path, archive_id))
        return archive_id

    def get_inventory(self, vault_name, sleep_interval=1200):
        vault = self.glacier_client.get_vault(vault_name)
        inventory_job_id = vault.retrieve_inventory()
        job = vault.get_job(inventory_job_id)
        while job.status_code == 'InProgress':
            job = vault.get_job(inventory_job_id)
            time.sleep(sleep_interval)
        inventory = job.get_output()
        return inventory

    def delete_archive(self, vault_name, archive):
        vault = self.glacier_client.get_vault(vault_name)
        self.__delete_archive(vault, archive)

    def __delete_archive(self, vault, archive_id):
        vault.delete_archive(archive_id)
        self.log.debug('Successfuly remove archive "{0}" from vault "{1}"'.format(archive_id, vault.name))
