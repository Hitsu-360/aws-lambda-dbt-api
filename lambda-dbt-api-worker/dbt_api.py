import requests
import json

LIMIT_PER_PAGE = 10

SUCCESS_STATUS = 10
ERROR_STATUS = 20

ENVIRONMENT_ID = 0000 #env_deploy (production environment) 

class dbt_api():

    def __init__(self, token, account, project):

        self.host = 'https://cloud.getdbt.com/api/v2'

        self.account = account
        self.project = project

        self.s = requests.Session()
        self.s.headers.update({'Accept': 'application/json','Authorization': f'Token {token}'})

    def get_jobs(self):

        response = self.s.get(f'{self.host}/accounts/{self.account}/jobs?project_id={self.project}&environment_id={ENVIRONMENT_ID}')

        json_response = json.loads(response.text)
    
        jobs = []

        if json_response['status']['code'] == 200:
            for data in json_response['data']:
                jobs.append(data)
    
        return jobs
        
    def get_job_by_id(self, id):

        response = self.s.get(f'{self.host}/accounts/{self.account}/jobs/{id}')

        json_response = json.loads(response.text)
    
        job = {}

        if json_response['status']['code'] == 200:
            if json_response['data']:
                job = json_response['data']
    
        return job

    def get_runs_by_job(self, job, offset, status):

        data = {}
        
        status_value = 0
        
        if status == 'success':
            status_value = SUCCESS_STATUS
        elif status == 'error':
            status_value = ERROR_STATUS

        runs = []
        hasMore = False

        response = self.s.get(f'{self.host}/accounts/{self.account}/runs?job_definition_id={job}&offset={offset}&limit={LIMIT_PER_PAGE}&status={status_value}&include_related=run_steps')

        if response:

            json_response = json.loads(response.text)

            if json_response['status']['code'] == 200:

                total = json_response['extra']['pagination']['total_count']

                if offset + LIMIT_PER_PAGE < total:  
                    hasMore = True

                for json_data in json_response['data']:
                    runs.append(json_data)

        data['runs'] = runs
        data['offset'] =  (offset + LIMIT_PER_PAGE) if (hasMore) else offset + len(runs)
        data['hasMore'] = hasMore

        return data
