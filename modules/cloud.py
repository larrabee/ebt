__author__ = 'larrabee'
import logging
import sys
from boto.glacier.layer2 import Layer2


class Amazon:
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name):
        self.log = logging.getLogger('__main__')
        self.glacier_client = Layer2(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
        
    def upload_file(self, vault_name, description, path):
        if self.is_vault_exist(vault_name):
            vault = self.glacier_client.get_vault(vault)
        else:
            vault = self.create_vault(vault_name)
        archive_id = self.__upload_file(vault=vault, description=description, path=path)
        return archive_id

    def create_vault(self, vault):
        vault = self.glacier_client.create_vault(name=vault)
        return vault

    def is_vault_exist(self, vault):
        vaults = [i.name for i in self.glacier_client.list_vaults()]
        if vault in vaults:
            return True
        else:
            return False

    def __upload_file(self, vault, description, path):
        archive_id = vault.concurrent_create_archive_from_file(path, description, part_size=4194304)
        self.log.debug('File "{0}" upload successfull. Archive id: "{1}"'.format(path, archive_id))
        return archive_id
