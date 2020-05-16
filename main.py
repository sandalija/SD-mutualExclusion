import pickle
import pywren_ibm_cloud as pywren
import ibm_boto3
import ibm_botocore
import time


BUCKET_NAME = 'deposit-sd-2020'
resultFile = 'result.txt'
askPermissionFile = 'p_write_'
grantPermissionFile = 'write_'
# key = 'f89332f9021e4b92988635666925e0cf'
key = '6710900afdbfe5372a57c54bf884b430378c69c4de868371'

N_SLAVES = 10 # TEST ONLY: Tiene que pedirse por teclado

def master(id, x, ibm_cos):     
    write_permission_list = [] 
    endCondition = False
    data = 'Nothing'
    while (not endCondition):
        time.sleep(x) # 1. monitor COS bucket each X seconds
        data = ibm_cos.list_objects(Bucket=BUCKET_NAME)
        endCondition = True
        # 2. List all "p_write_{id}" files
        # 3. Order objects by time of creation
        # 4. Pop first object of the list "p_write_{id}" 
        # 5. Write empty "write_{id}" object into COS
        # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
        # 7. Monitor "result.json" object each X seconds until it is updated     
        # 8. Delete from COS “write_{id}”
        # 8. Back to step 1 until no "p_write_{id}" objects in the bucket
    return data 

def slave(id, x, ibm_cos): 
    empty = []
    name = askPermissionFile + str(id)
    ibm_cos.put_object(BUCKET_NAME, name, empty)
    permision = grantPermissionFile + str(id)
    success = False
    while not success:
        try:
            filew = ibm_cos.get_object(BUCKET_NAME, permision)
            success = True
            break
        except pywren.storage.utils.StorageNoSuchKeyError:
            time.sleep(x)
    resultData = ibm_cos.get_object(BUCKET_NAME, resultFile)
    resultData = pickle.load(resultData)
    resultData.append(id)
    serialized = pickle.dumps(resultFile, protocol=0) # protocol 0 is printable ASCII
    ibm_cos.put_object(BUCKET_NAME, resultFile, serialized)
    # No need to return anything """

if __name__ == '__main__':
   pw = pywren.ibm_cf_executor()
   pw.call_async(master, 0)
   pw.map(slave, range(N_SLAVES))
   write_permission_list = pw.get_result()

   # Get result.txt
# check if content of result.txt == write_permission_list