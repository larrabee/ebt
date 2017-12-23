import logging
from datetime import datetime, timedelta
import ebt_cloud


log = logging.getLogger('__main__')


class CleanUpGlacier(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, vault, dayexp=365):
        self.dayexp = dayexp
        self.vault = vault
        self.glacier = ebt_cloud.Amazon(aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key,
                                        region_name=region_name)

    def __check_archive_expiration(self, archive):
        archive_id = archive['ArchiveId']
        creation_date = datetime.strptime(archive['CreationDate'], "%Y-%m-%dT%H:%M:%SZ")
        description = archive['ArchiveDescription']
        size_gb = int(archive['Size'] / (1024 * 1024 * 1024))
        if creation_date + timedelta(days=self.dayexp) <= datetime.now():
            log.info('Remove archive. ID: "{0}", Description: "{1}", Creation date: "{2}", Size in GB: "{3}"'.format(
                archive_id, description, creation_date, size_gb
            ))
            return True
        else:
            log.info('Skip archive. ID: "{0}", Description: "{1}", Creation date: "{2}", Size in GB: "{3}"'.format(
                archive_id, description, creation_date, size_gb
            ))
            return False
        pass

    def start(self):
        log.info('Cleanup vault "{0}"'.format(self.vault))
        inventory = self.glacier.get_inventory(self.vault)
        for archive in inventory['ArchiveList']:
            if self.__check_archive_expiration(archive) is True:
                self.glacier.delete_archive(vault_name=self.vault, archive=archive['ArchiveId'])

