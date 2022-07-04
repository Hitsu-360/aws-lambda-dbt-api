import json 
import requests
import pandas

LIMIT_PER_PAGE = 10

graphql_queries = {
    "exposures": '''
    {
        exposures(jobId: #jobId#) {
            uniqueId
            runId
            jobId
            environmentId
            projectId
            accountId
            name
            description
            resourceType
            ownerName
            ownerEmail
            url
            parentsSources {
                uniqueId
            }
            parentsModels {
                uniqueId
            }
        }
    }
    ''',
    "models": '''
    {
        models(jobId: #jobId#) {
            uniqueId
            runId
            jobId
            environmentId
            projectId
            accountId
            name
            error
            schema
            status
            skip
            executionTime
            executeStartedAt
            executeCompletedAt
            tests {
                uniqueId
              	runId
              	accountId
              	projectId
              	environmentId
              	jobId
              	name
              	state
              	status
              	error
              	fail
              	warn
            }
        }
    }
    ''',
    "sources": '''
    {
        sources(jobId: #jobId#) {
            uniqueId
            runId
            jobId
            environmentId
            projectId
            accountId
            name
            sourceName
            sourceDescription
            state
            maxLoadedAt
            snapshottedAt
            runGeneratedAt
            runElapsedTime
            criteria {
                warnAfter {
                period
                count
                }
                errorAfter {
                period
                count
                }
            }
            tests {
                uniqueId
              	runId
              	accountId
              	projectId
              	environmentId
              	jobId
              	name
              	state
              	status
              	error
              	fail
              	warn
            }
        }
    }
    '''
}

class dbt_metadata_api: 

    def __init__(self, dbt_api_token):
        
        self.url = 'https://metadata.cloud.getdbt.com/graphql'
        self.headers = {'Authorization': f'Bearer {dbt_api_token}'}

    def get_resource(self, resource_type, job_id, resource_list = None):

        if resource_list:
            
            resources_response = []
            
            for resource_name in resource_list:
                
                resource_response = self.get_dbt_metadata_resource(resource_type, job_id, resource_name) 
            
                resources_response.append(resource_response)
                
            return resources_response
            
        else:
            
            return self.get_dbt_metadata_resource(resource_type, job_id)
    
    def get_dbt_metadata_resource(self, resource_type, job_id, resource_name = None):
        
        query = graphql_queries[resource_type]

        query = query.replace('#jobId#', job_id)

        if resource_name:
            query = query.replace(f'#{resource_type}_name#', resource_name)
            
        print(f'JobId: {job_id} | Resource_type: {resource_type}')

        r = requests.post(url=self.url, json={ "query": query}, headers=self.headers)
        
        json_response = json.loads(r.text)

        resource = json_response["data"][resource_type]

        return resource
            


