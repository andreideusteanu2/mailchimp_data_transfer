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
    
    import mysql.connector.errors as myerr
    
    from tenacity import retry,wait_fixed,wait_random,retry_if_exception_type,stop_after_attempt
#
    @retry(wait=wait_fixed(10)+wait_random(0,3)
        ,retry=retry_if_exception_type(myerr.InterfaceError)
        ,stop=stop_after_attempt(3))
#    
    def connect_to_trinity_mysql(self):
        from json import loads
        
        f=open(str(self.credentialsPath/"2performantdb_trinityReplica_config.json"))
        dbConfig=loads(f.read())
        f.close()
        
        from mysql.connector import connect
        cnx=connect(user=dbConfig['user'],password=dbConfig['password']
                    ,host=dbConfig['host'],database=dbConfig['database']
                    ,connect_timeout=12600,use_pure=True)
        
        
        return cnx
        
    def get_cursor(self,cnx):
        import mysql.connector
        try:
            cnx.ping(reconnect=True, attempts=3, delay=5)
        except mysql.connector.Error as err:
            # reconnect your cursor as you did in __init__ or wherever    
            cnx = self.connect_to_trinity_mysql()
        return cnx.cursor()
            
        
    
    def connect_to_gcs(self):
        from google.cloud import storage
        credentials=self.credentialsPath/'2Performant-driveAccessService.json'
        storage_client = storage.Client.from_service_account_json(str(credentials))
        
        return storage_client
    
    def connect_to_gbq(self):
        from google.cloud import bigquery
        credentials=self.credentialsPath/'2Performant-driveAccessService.json'
        bigquery_client=bigquery.Client.from_service_account_json(str(credentials))
        
        return bigquery_client
    
    def connect_to_gdrive(self):
        from pydrive.auth import GoogleAuth
        from pydrive.drive import GoogleDrive
        from oauth2client.service_account import ServiceAccountCredentials
        gauth = GoogleAuth()
        scope = ['https://www.googleapis.com/auth/drive']
        
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(str(self.credentialsPath/'2Performant-driveAccessService.json'), scope)
        gdrive_client = GoogleDrive(gauth)
    
        return gdrive_client
    
    def connect_to_mailchimp(self):
        import requests as r
        from json import loads
        user='anystring'
        with open(str(self.credentialsPath/'mailchimp_api_key.json'),'r') as f:
            key=loads(f.readline())['key']
        baseUrl='https://us1.api.mailchimp.com/3.0/'
        resp=r.get(baseUrl,auth=(user,key))
        
        return resp.json()