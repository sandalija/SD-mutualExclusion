import json
import pywren_ibm_cloud as pywren
import ibm_boto3
import ibm_botocore
import time
import json
from ibm_botocore.exceptions import ClientError
import sys


BUCKET_NAME = 'deposit-sd-2020'
resultFile = 'result.txt'
askPermissionFile = 'p_write_'
grantPermissionFile = 'write_'

def master(id, x, ibm_cos):
    data = []
    res = json.dumps('IDs:')
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=resultFile, Body=res)
    resultTime = ibm_cos.list_objects_v2(Bucket=BUCKET_NAME, Prefix=resultFile)['Contents'][0]['LastModified']
    empty = True
    while empty is True:
        # 1. monitor COS bucket each X seconds
        time.sleep(x)
        # 2. List all "p_write_{id}" files
        files = ibm_cos.list_objects_v2(Bucket=BUCKET_NAME, Prefix=askPermissionFile)
        elem = files['KeyCount']
        empty = elem == 0    
    elem0 = False
    while elem0 is False:
        # TO REMOVE: files = ibm_cos.list_objects_v2(Bucket=BUCKET_NAME, Prefix=askPermissionFile)['Contents']
        # 3. Order objects by time of creation
        bucket_content = []
        for elem in files:
            bucket_content.append({"Key": elem['Key'], "LastModified":  elem['LastModified']})
        bucket_content = sorted(bucket_content, key=lambda k: k['LastModified'])
        # 4. Pop first object of the list "p_write_{id}" 
        askedPermission = bucket_content.pop()['Key']
        # 5. Write empty "write_{id}" object into COS
        grantPermission = askedPermission.replace(askPermissionFile, '')
        data.append(grantPermission)
        grantPermission = grantPermissionFile + str(grantPermission)
        # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=askedPermission)  
        ibm_cos.put_object(Bucket=BUCKET_NAME, Key=grantPermission)
        hasChanged = False
        while not hasChanged:
            newResultTime = ibm_cos.list_objects_v2(Bucket=BUCKET_NAME, Prefix=resultFile)['Contents'][0]['LastModified']
            hasChanged = newResultTime != resultTime
            if hasChanged: resultTime = newResultTime
            else: time.sleep(x)
        # 8. Delete from COS “write_{id}”
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key=grantPermission) 
        elem = ibm_cos.list_objects_v2(Bucket=BUCKET_NAME, Prefix=askPermissionFile)['KeyCount']
        data.append(elem)
        # 9. Back to step 1 until no "p_write_{id}" objects 
        elem0 = elem == 0
        if elem == 0:
            break
    return data

def slave(id, x, ibm_cos):
    # 1. Write empty "p_write_{id}" object into COS
    name = askPermissionFile + str(id)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=name, Body='')
    permision = grantPermissionFile + str(id)
    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    success = False
    while not success:
        try:
            filew = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=permision)
            success = True
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                time.sleep(x)
    # 3. If write_{id} is in COS: get result.json, append {id}, and put back to COS result.json
    resultData = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=resultFile)
    data = resultData['Body'].read()
    x = json.loads(data)
    x = x + '\n' + str(id)
    serial = json.dumps(x)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=resultFile, Body=serial)
    return None
    # No need to return anything """

if __name__ == '__main__':
    # Check arg
    if ((len(sys.argv) - 1) == 0): N_SLAVES = 2
    elif (int(sys.argv[1]) > 100): N_SLAVES = 2
    else: N_SLAVES = int(sys.argv[1])
    start_time = time.time()
    pw = pywren.ibm_cf_executor()
    pw.call_async(master, 0)
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()
    elapsed_time = time.time() - start_time
    print("Seconds: %.3f" % elapsed_time)
    # Get result.json
    ibm_cos = pw.internal_storage.get_client()
    # check if content of result.json == write_permission_list
    result = ibm_cos.get_object(Bucket=BUCKET_NAME, Key=resultFile)['Body'].read()
    res = json.loads(result)
    print ("result.txt file:")
    print (res)
    print ("\nlist from master:")
    for n in write_permission_list:
        print (n) 

