#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 30 15:53:04 2018

@author: developer
"""

class Connector:
   
    def __init__(self):
        import determine_running_folder_path as drfp
        pathFiles=drfp.main()
        credentialsPath=pathFiles/'credentials'
        self.credentialsPath=credentialsPath      
        
    def connect_to_gcs(self,serviceAccountFileName):
        from google.cloud import storage
        credentials=self.credentialsPath/serviceAccountFileName
        storage_client = storage.Client.from_service_account_json(str(credentials))
        
        return storage_client
    
    def connect_to_gbq(self,serviceAccountFileName):
        from google.cloud import bigquery
        credentials=self.credentialsPath/serviceAccountFileName
        bigquery_client=bigquery.Client.from_service_account_json(str(credentials))
        
        return bigquery_client
    
    def connect_to_mailchimp(self,apiKeyFileName):
        import requests as r
        from json import loads
        user='anystring'
        with open(str(self.credentialsPath/apiKeyFileName),'r') as f:
            key=loads(f.readline())['key']
        baseUrl='https://us1.api.mailchimp.com/3.0/'
        resp=r.get(baseUrl,auth=(user,key))
        
        return resp.json()
