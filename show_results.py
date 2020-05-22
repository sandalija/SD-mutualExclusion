import pickle

import pickle
import pywren_ibm_cloud as pywren
import ibm_boto3
import ibm_botocore
import time
import json
from ibm_botocore.exceptions import ClientError


BUCKET_NAME = 'deposit-sd-2020'
resultFile = 'result.txt'
askPermissionFile = 'p_write_'
grantPermissionFile = 'write_'
# key = 'f89332f9021e4b92988635666925e0cf'
#key = '6710900afdbfe5372a57c54bf884b430378c69c4de868371'
key = 'bc586c5b02fc0842160010ce6bfb54b529fd7e82003ecd0b'

N_SLAVES = 1 # TEST ONLY: Tiene que pedirse por teclado

def master(id, x, ibm_cos):
    askPermissionFileB = 'p_write_0'
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=askPermissionFileB, Body='')

    
    files = ibm_cos.list_objects_v2(Bucket=BUCKET_NAME, Prefix=askPermissionFile)['Contents']

    """ time.sleep(5)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=askPermissionFileB, Body='')
    # empty = []
    # res = pickle.dumps(empty, protocol=0)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=resultFile, Body='')
     # 1. monitor COS bucket each X seconds
    data = []
     # 2. List all "p_write_{id}" files
    # dictionario de los archivos: data.append(a['Contents'])
    bucket_content = []
    for elem in files:
        bucket_content.append({"Key": elem['Key'], "LastModified":  elem['LastModified']})
    data.append('2')

    # 3. Order objects by time of creation
    bucket_content = sorted(bucket_content, key=lambda k: k['LastModified'])
    data.append('3')

    # 4. Pop first object of the list "p_write_{id}" 
    askedPermission = bucket_content.pop()['Key']
    data.append('4')

    # 5. Write empty "write_{id}" object into COS
    grantPermission = askedPermission.replace(askPermissionFile, '')
    grantPermission = grantPermissionFile + str(grantPermission)
    data.append(grantPermission)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=grantPermission)
    data.append('5')

    # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
    ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=askedPermission)
    data.append('6')

    # 7. Monitor "result.json" object each X seconds until it is updated  
    hasChanged = True
    while not hasChanged:
        try:
            filesToMonitor = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix=askPermissionFile)['Contents']
            hasChanged = files.items == filesToMonitor.items()
            time.sleep(x)
        except KeyError:
            hasChanged = True
            data.append('key error')   
    # 8. Delete from COS “write_{id}”
    ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=grantPermission)
    # 9. Back to step 1 until no "p_write_{id}" objects in the bucket
    # files = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix=askPermissionFile)['Contents'] """
    
    time.sleep(x)
    return files

def slave(id, x, ibm_cos): 
    name = askPermissionFile + str(id)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=name, Body='')
    permision = grantPermissionFile + str(id)
    success = False
    while not success:
        try:
            filew = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=permision)
            success = True
            break
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                time.sleep(x)
    """ resultD = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=resultFile)
    resultData = pickle.loads(resultD)
    resultData.append(id)
    serialized = pickle.dumps(resultData, protocol=0) # protocol 0 is printable ASCII """
    resultData = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=resultFile)
    """ resultD = resultData['Body'].read()
    resultData = pickle.loads(resultD)
    resultData.append(id) """
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=resultFile, Body='Hola')
    # No need to return anything """

if __name__ == '__main__':
   pw = pywren.ibm_cf_executor()
   pw.call_async(master, 0)
   # pw.map(slave, range(N_SLAVES))
   print("OK")
   write_permission_list = pw.get_result()
   print(write_permission_list) # TEST ONLY

   # Get result.txt
    # check if content of result.txt == write_permission_list