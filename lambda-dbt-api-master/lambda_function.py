import json
import boto3
import time
import datetime
import botocore

secrets_client = boto3.client(
    'secretsmanager',
    region_name='<your-region>'
)

credentials = json.loads(secrets_client.get_secret_value(SecretId='credentials/<dbt-api-credentials>')['SecretString'])

# AWS
BUCKET = credentials['BUCKET']
AWS_REGION = credentials['AWS_REGION']
LAMBDA_WORKER = '<lambda-dbt-api-worker-arn>'

S3 = boto3.resource(
    's3',
    region_name = AWS_REGION
)

config = botocore.config.Config(
    read_timeout=900,
    connect_timeout=900,
    retries={"max_attempts": 0}
)

LAMBDA = boto3.client(
    service_name = 'lambda',
    region_name = AWS_REGION  ,
    config = config
)

def load_s3_file_content(s3_path, s3_file_content):
     # Init bucket object
    bucket = S3.Bucket(BUCKET)
    # Write to file    
    bucket.put_object(Key=s3_path, Body=s3_file_content)

def get_s3_file_content(s3_path):
     # Init bucket object

    if not s3_file_exists(s3_path):
        load_s3_file_content(s3_path, '')

    obj = S3.Object(BUCKET,s3_path)

    # Get content
    content = obj.get()['Body'].read().decode('utf-8') 

    return content

def s3_file_exists(s3_path):

    bucket = S3.Bucket(BUCKET)

    if len(list(bucket.objects.filter(Prefix=s3_path))) > 0:
        return True
    
    return False

def lambda_invoke(function, invoke_type, payload):

    if credentials['DEBUG'] == True:
        return {}

    return LAMBDA.invoke(FunctionName=function, InvocationType=invoke_type, Payload=payload)
    
def handle_invoke_dbt_runs(request_payload):

    hasMore = True

    while(hasMore):

        lambda_worker_response = lambda_invoke(LAMBDA_WORKER, 'RequestResponse', json.dumps(request_payload))

        if lambda_worker_response:

            if lambda_worker_response.get('Payload'):

                response_payload = json.loads(json.loads(lambda_worker_response['Payload'].read()))

                print(response_payload)

                hasMore = response_payload['hasMore']
        else:
            hasMore = False

def load_dbt_resources_by_job(job, load_exposures = True):

    request_payload = {
        "worker_type": "runs",
        "job": {
            "id": job['id'],
            "name": job['name']
        }
    } 
    
    #loading runs with "success" status data from the dbt cloud api
    request_payload["job"]["status"] = "success"
    
    handle_invoke_dbt_runs(request_payload)
    
    #loading runs with "error" status data from the dbt cloud api
    request_payload["job"]["status"] = "error"
    
    handle_invoke_dbt_runs(request_payload)
    
    #loading models data from the dbt cloud metadata api
    request_payload['worker_type'] = 'models'
    
    lambda_invoke(LAMBDA_WORKER, 'RequestResponse', json.dumps(request_payload))
    
    #loading sources data from the dbt cloud metadata api
    request_payload['worker_type'] = 'sources'
    
    lambda_invoke(LAMBDA_WORKER, 'RequestResponse', json.dumps(request_payload))
    
    #loading exposures data from the dbt cloud metadata api
    request_payload['worker_type'] = 'exposures'

    lambda_invoke(LAMBDA_WORKER, 'RequestResponse', json.dumps(request_payload))
    
    

def lambda_handler(request, context):
    
    jobs_payload = {}
    
    jobs_payload['worker_type'] = 'jobs'
    
    # If a job id was passed as parameter
    if request.get('job'):
        jobs_payload['job'] = request['job']

    lambda_worker_jobs_response = lambda_invoke(LAMBDA_WORKER, 'RequestResponse', json.dumps(jobs_payload))

    if lambda_worker_jobs_response:

        if lambda_worker_jobs_response.get('Payload'):

            jobs_payload = json.loads(json.loads(lambda_worker_jobs_response['Payload'].read()))
            
            # All jobs payload
            if jobs_payload.get('jobs'):

                jobs = jobs_payload['jobs']

                for job in jobs:
                    
                    load_dbt_resources_by_job(job)
            
            # Job payload based on the parameter giving
            elif jobs_payload.get('job'):
                
                job = jobs_payload.get('job')
                
                load_dbt_resources_by_job(job)
                   
    return {"state": {}}

if __name__ == '__main__':
    lambda_handler({"state": {}}, None)