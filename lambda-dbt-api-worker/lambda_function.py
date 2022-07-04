from dbt_api import dbt_api
from dbt_metadata_api import dbt_metadata_api

import json
import boto3
import time
import datetime
import pandas

secrets_client = boto3.client(
    'secretsmanager',
    region_name='<your-region>'
)

credentials = json.loads(secrets_client.get_secret_value(SecretId='credentials/<dbt-api-credentials>')['SecretString'])

# DBT
DBT_API_KEY = credentials['DBT_API_KEY']
DBT_ACCOUNT = credentials['DBT_ACCOUNT']
DBT_PROJECT = credentials['DBT_PROJECT']

# AWS
BUCKET = credentials['BUCKET']
AWS_REGION = credentials['AWS_REGION']

JOB_STATE_FILE = 'job_state.json'
JOBS_DEFINITIONS_FOLDER = 'jobs_difinitions'
JOBS_DEFINITIONS_FILE = 'jobs_difinitions.json'

S3 = boto3.resource(
    's3',
    region_name = AWS_REGION
)

def get_s3_path_by_job(job):
    
    if job:
        job_id = str(job['id'])
        job_name = job['name'].replace(' ','_').replace(',','').lower()
        
        path = f'job_{job_id}_{job_name}'
        
        return path
    
    return 'error'

def load_s3_file_content(s3_path, s3_file_content):
     # Init bucket object
    bucket = S3.Bucket(BUCKET)
    
    final_path = 'dbt_api/' + s3_path
    
    # Write to file    
    bucket.put_object(Key=final_path, Body=s3_file_content)

def get_s3_file_content(s3_path):
     # Init bucket object

    if not s3_file_exists('dbt_api/' + s3_path):
        load_s3_file_content(s3_path, '')

    obj = S3.Object(BUCKET,'dbt_api/' + s3_path)

    # Get content
    content = obj.get()['Body'].read().decode('utf-8') 

    return content

def s3_file_exists(s3_path):

    bucket = S3.Bucket(BUCKET)

    if len(list(bucket.objects.filter(Prefix=s3_path))) > 0:
        return True
    
    return False

def load_jobs(jobs):

    if len(jobs) > 0:

        s3_path = f'{JOBS_DEFINITIONS_FOLDER}/{JOBS_DEFINITIONS_FILE}'

        load_s3_file_content(s3_path,json.dumps(jobs, indent = 2))


def load_runs_by_job(dbt, job_state): 
    
    job_id = job_state['id']
    job_status = job_state['status']

    data = dbt.get_runs_by_job(job_id, job_state['offsets'][job_status], job_status)

    runs = data['runs']
    offset = data['offset']
    hasMore = data['hasMore']

    if len(runs) > 0:

        job_name = job_state['name'].replace(' ', '_').replace(',','')
        current_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H:%M:%S')

        s3_path = f'{get_s3_path_by_job(job_state)}/job_{job_name}_{current_timestamp}.json'

        load_s3_file_content(s3_path,json.dumps(runs, indent = 2))

    job_state['offsets'][job_status] = offset
    job_state['hasMore'] = hasMore    

    return job_state

def get_jobs(dbt):
    jobs = dbt.get_jobs()

    load_jobs(jobs)

    return json.dumps({"jobs": jobs}, indent=4)
    
def get_job(dbt, job):
    job_response = dbt.get_job_by_id(job['id'])

    return json.dumps({"job": job_response}, indent=4)

def get_runs_by_job(dbt,job):

    if job:

        current_job_state = create_jobs_state(job)

        new_job_state = load_runs_by_job(dbt, current_job_state)

        update_job_state(new_job_state)

        return json.dumps(new_job_state, indent=4)

    return ''
    
def load_exposures_list(): 

    f = open('dbt_metadata_api_config.json',) 
    
    data = json.load(f) 

    exposures = data['exposures']

    f.close() 

    return exposures
    
def get_metadata_resource_by_job(dbt_metadata_api, resource_type, job, resource_list = None):
    
    job_id = str(job['id'])
        
    job_name = job['name'].replace(' ','_').lower()
    
    resources_response = dbt_metadata_api.get_resource(resource_type, job_id, resource_list)
    
    if len(resources_response) > 0:

        df = pandas.read_json(json.dumps(resources_response))
    
        csv = df.to_csv(index=False , encoding='utf-8')
        
        s3_path = f'{get_s3_path_by_job(job)}/{resource_type}.csv'
        
        load_s3_file_content(s3_path,csv)
    
        print('File loaded to {' + s3_path + '}')
    
    else: 
        
        print(f'API returned empty for the job ({job_id} - {job_name})')
    
    return {}
    
    
def create_jobs_state(job):

    # Get bucket jobs states file content
    job_id = str(job['id'])
    job_name = job['name'].replace(' ','_').lower()
    content = get_s3_file_content(f'{get_s3_path_by_job(job)}/{JOB_STATE_FILE}')

    job_state = {}
    
    # Load existent ids 
    if content != '{}' and content != '':

        job_state = json.loads(content)
        job_state['status'] = job['status']

    else:
        job_state['id'] = job['id']
        job_state['name'] = job['name']
        job_state['status'] = job['status']
        job_state['offsets'] = {}
        job_state['offsets']['success'] = 0
        job_state['offsets']['error'] = 0
        job_state['hasMore'] = False
        

    # Update bucket jobs states file 
    load_s3_file_content(f'{get_s3_path_by_job(job)}/{JOB_STATE_FILE}', json.dumps(job_state, indent = 2))

    return job_state

def update_job_state(new_job_state):

    load_s3_file_content(f'{get_s3_path_by_job(new_job_state)}/{JOB_STATE_FILE}', json.dumps(new_job_state, indent = 2))

def lambda_handler(request, context):

    if request:

        dbt_logs = dbt_api(DBT_API_KEY, DBT_ACCOUNT, DBT_PROJECT)
        dbt_metadata = dbt_metadata_api(DBT_API_KEY)

        if request.get('worker_type'):

            if request['worker_type'] == 'jobs':
                
                if request.get('job'):
                    
                    return get_job(dbt_logs, request['job'])
                    
                else:
                    
                    return get_jobs(dbt_logs)

            elif request['worker_type'] == 'runs':

                if request.get('job'):

                    return get_runs_by_job(dbt_logs, request['job'])
                    
            elif request['worker_type'] == 'models':
                
                if request.get('job'):
                    
                    return get_metadata_resource_by_job(dbt_metadata,"models",request['job'])

            elif request['worker_type'] == 'sources':
                
                if request.get('job'):
                    
                    return get_metadata_resource_by_job(dbt_metadata,"sources",request['job'])
                    
            elif request['worker_type'] == 'exposures':
                
                if request.get('job'):
                    
                    return get_metadata_resource_by_job(dbt_metadata,"exposures",request['job'])
                
            
# debugging
if __name__ == '__main__':

    # worker_type runs
    mock_request = {
        "worker_type": "models",
        "job": {
            "id": 1111,
            "name": "Job Example"
        }
    } 

    print(lambda_handler(mock_request, None))