import pickle
import pywren_ibm_cloud as pywren
import ibm_boto3
import ibm_botocore
import time
import json
from ibm_botocore.exceptions import ClientError


BUCKET_NAME = 'javiermartinezbucket1'
resultFile = 'result.txt'
askPermissionFile = 'p_write_'
grantPermissionFile = 'write_'
# key = 'f89332f9021e4b92988635666925e0cf'
#key = '6710900afdbfe5372a57c54bf884b430378c69c4de868371'
key = 'bc586c5b02fc0842160010ce6bfb54b529fd7e82003ecd0b'

N_SLAVES = 1 # TEST ONLY: Tiene que pedirse por teclado

def master(id, x, ibm_cos):
    # askPermissionFileB = 'p_write_1'
    time.sleep(10)
    # ibm_cos.put_object(Bucket=BUCKET_NAME, Key=askPermissionFileB, Body='')
    # empty = []
    res = pickle.dumps('IDs:', protocol=0)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=resultFile, Body=res)
    resultTime = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix=resultFile)['Contents'][0]['LastModified']
     # 1. monitor COS bucket each X seconds
    data = []
    try:
        files = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix=askPermissionFile)['Contents']

        bucket_content = []
        for elem in files:
            bucket_content.append({"Key": elem['Key'], "LastModified":  elem['LastModified']})
    
        bucket_content = sorted(bucket_content, key=lambda k: k['LastModified'])

        askedPermission = bucket_content.pop()['Key']

        grantPermission = askedPermission.replace(askPermissionFile, '')
        grantPermission = grantPermissionFile + str(grantPermission)
        data.append(grantPermission)
        ibm_cos.put_object(Bucket=BUCKET_NAME, Key=grantPermission)

        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=askedPermission)

        hasChanged = False
        while not hasChanged:
            newResultTime = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix=resultFile)['Contents'][0]['LastModified']
            hasChanged = newResultTime == resultTime
            if hasChanged: resultTime = newResultTime
            else: time.sleep(x)
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=grantPermission)
        time.sleep(x)
        return data
    except Exception as e:
        data.append(e)
        return data

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
    resultData = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=resultFile)
    data = resultData['Body'].read()
    f5 = pickle.loads(data)
    f5 = f5 + '\n' + str(id)
    serial = pickle.dumps(f5, protocol=0)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=resultFile, Body=serial)
    # No need to return anything """

if __name__ == '__main__':
   pw = pywren.ibm_cf_executor()
   pw.call_async(master, 0)
   pw.map(slave, range(N_SLAVES))
   write_permission_list = pw.get_result()
   print(write_permission_list) # TEST ONLY
   ibm_cos = pw.internal_storage.get_client()
   result = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=resultFile)['Body'].read()
   res = pickle.loads(result)
   print(res)
   # Get result.txt
    # check if content of result.txt == write_permission_list