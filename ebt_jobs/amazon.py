import logging
from datetime import datetime, timedelta
import ebt_cloud
import os


log = logging.getLogger('__main__')


class CleanUpGlacier(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, vault, dayexp=365):
        self.dayexp = dayexp
        self.vault = vault
        self.glacier = ebt_cloud.AmazonGlacier(aws_access_key_id=aws_access_key_id,
                                               aws_secret_access_key=aws_secret_access_key,
                                               region_name=region_name)

    def _check_archive_expiration(self, archive):
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
            if self._check_archive_expiration(archive) is True:
                self.glacier.delete_archive(vault_name=self.vault, archive_id=archive['ArchiveId'])


class RetrieveArchive(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, vault, dest_dir):
        self.dest_dir = dest_dir
        self.vault = vault
        self.glacier = ebt_cloud.AmazonGlacier(aws_access_key_id=aws_access_key_id,
                                               aws_secret_access_key=aws_secret_access_key,
                                               region_name=region_name)

    def get_file(self, archive_id, filename=None):
        if filename in None:
            dest = os.path.join(self.dest_dir, archive_id)
        else:
            dest = os.path.join(self.dest_dir, filename)
        log.info("Download archive {0} to {1}".format(archive_id, dest))
        self.glacier.download_file(self.vault, archive_id, dest)

