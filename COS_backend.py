import ibm_boto3
import ibm_botocore

class COS_Backend:
    """
    A wrap-ip around COS ibm_boto3 APIs
    """
    def __init__(self):
        access_key = 'f89332f9021e4b92988635666925e0cf'
        secret_key = '6710900afdbfe5372a57c54bf884b430378c69c4de868371'
        service_endpoint = 'https://s3.eu-de.cloud-object-storage.appdomain.cloud'
        client_config =  ibm_botocore.client.Config(max_pool_connections = 200,
                                                     user_agent_extra = 'pywren-ibm-cloud')

        self.cos_client = ibm_boto3.client('s3',
                                            aws_access_key_id=access_key,
                                            aws_secret_access_key =  secret_key,
                                            config = client_config,
                                            endpoint_url =  service_endpoint)

    def put_object(self, bucket_name, key, data):
        """
        Put an object in COS. Override the object if the key alreafy exists.
        :param key: key of the object
        :param data: data of the object
        :type data: str/bytes
        :return: None
        """
        try:
            res = self.cos_client.put_object(Bucket=bucket_name, Key = key, Body = data)
            status = "OK" if res['ResponseMetadata']['HTTPStatusCode'] == 200 else 'Error'
            try:
                print('PUT object {} - Size: {} - {}'.format(key, sizeof_fmt(len(data)),status))
            except:
                print('PUT object {} {}'.format(key, status))
        except ibm_botocore.exceptions.ClientError as e:
                raise e

    def get_object(self, bucket_name, key, stream = False, extra_get_args={}):
        """
        Get object from COS with a key. Throws StorageNoSuchKeyError if thr given key does not exist.
        :param key: key of the object
        :return Data of the object
        :rtype: str/bytes
        """
        try:
            r = self.cos_client.get_object(Bucket = bucket_name, Key = key, **extra_get_args)
            if stream:
                data = r['Body']
            else:
                data = r['Body'].read()
            return data
        except ibm_botocore.exceptions.ClientError as e:
            raise e

    def head_object(self, bucket_name, key):
        try:
            metadata = self.cos_client.head_object(Bucket = bucket_name, Key = key)
            return metadata['ResponseMetadata']['HTTPHeaders']
        except ibm_botocore.exceptions.ClientError as e:
            raise e

    def delete_object(self, bucket_name, key):
        return self.cos_client.delete_object(Bucket = bucket_name, Key = key)

    def list_object(self, bucket_name, prefix=None):
        paginator = self.cos_client.get_paginator('list_objects_v2')
        try:
            if (prefix is not None):
                page_paginator = paginator.paginate(Buckey = bucket_name, Prefix = prefix)
            else:
                page_paginator =  paginator.paginate(Bucket = bucket_name)
            object_list = []
            for page in page_paginator:
                if 'Contents' in page:
                    for item in page['Contents']:
                        object_list.append(item)
            return object_list
        except ibm_botocore.exceptions.ClientError as e:
            raise e


    
