__author__ = 'larrabee'
import logging
import sys
import boto3


class Amazon:
    def __init__(aws_access_key_id, aws_secret_access_key, region_name):
        self.log = logging.getLogger('__main__')
        self.glacier_client = boto3.client('glacier', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
        
    def upload_file(self, vault, description, path):
        response = self.glacier_client.upload_archive(vaultName=vault, archiveDescription=description, body=open(path, 'rb'))
        self.log.debug('File "{0}" upload successfull. Archive id: "{1}"'.format(path, response['archiveId']))
        return response

    
