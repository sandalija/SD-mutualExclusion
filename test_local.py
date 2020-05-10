import pickle
import pywren_ibm_cloud as pywren
import ibm_boto3
import ibm_botocore
import time
import json


BUCKET_NAME = 'deposit-sd-2020'
resultFile = 'result.txt'
askPermissionFile = 'p_write_'
grantPermissionFile = 'write_'
# key = 'f89332f9021e4b92988635666925e0cf'
key = '6710900afdbfe5372a57c54bf884b430378c69c4de868371'

N_SLAVES = 10 # TEST ONLY: Tiene que pedirse por tecladodef master(id, x, ibm_cos):     

def master(id, x, ibm_cos):
    write_permission_list = [] 
    endCondition = False
    data = []
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key=resultFile)
    while (not endCondition):
        time.sleep(x) # 1. monitor COS bucket each X seconds
        try:
            i = 1
            try:
                data.append(ibm_cos.list_object(Bucket=BUCKET_NAME))
            except:
                data.append("No hay más ficheros por leer")
            # 2. List all "p_write_{id}" files
            # 3. Order objects by time of creation
            # 4. Pop first object of the list "p_write_{id}" 
            # 5. Write empty "write_{id}" object into COS
            # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
            # 7. Monitor "result.json" object each X seconds until it is updated     
            # 8. Delete from COS “write_{id}”
            # 8. Back to step 1 until no "p_write_{id}" objects in the bucket
            endCondition = True
        except:
            data = 'Tienes que crear el archivo result.txt'
        endCondition = True # TEST ONLY
        
    return data 

pw = pywren.ibm_cf_executor()
params = 4
pw.call_async(master, params)
print (pw.get_result())