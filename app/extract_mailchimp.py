#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 13:13:15 2019

@author: developer
"""

import requests as r
from copy import deepcopy
from multiprocess import Pool

baseUrl='https://us1.api.mailchimp.com/3.0/'

def create_operations_array(baseUrl,method,mapper,total_items):    
    batchSize=0
    mapper['params']['count']=500
    mapper['params'].pop('offset', None)
    mapper['params']
    operations_array=[]
    while batchSize<=total_items:
        if batchSize>0:
            mapper['params']['offset']=0
            mapper['params']['offset']=batchSize-1
        batchSize+=500
        operation={}
        operation={'method':method
                   ,'path':mapper.get('request_suffix')
                   ,'params':mapper.get('params')
                   ,'list_name':mapper.get('list_name')
                   }
        operations_array.append(deepcopy(operation))
        
    return operations_array
    
    
def get_total_items(request_url,auth):
    total_items=(r.get(request_url
                      ,auth=auth
                      ,params={'fields':['total_items']
                      ,'count':1}
                      ).json()
                    )
    
    return total_items.get('total_items')

    
def copy_set_mapper_with_campaign_id(campaign_id,mapperIn):
    mapperOut=deepcopy(mapperIn)
    mapperOut['request_suffix']=mapperOut['request_suffix'].replace('[__campaign_id__]',campaign_id)
    
    return mapperOut

def send_request_to_api(baseUrl,auth,operation):
    response=r.request(method=operation.get('method')
                    ,url=baseUrl+operation.get('path')
                    ,auth=auth
                    ,params=operation.get('params')).json()
    
    out=response.get(operation.get('list_name'))
    
    return out

    
def get_all_data(mapper,auth):
    request_url=baseUrl+mapper.get('request_suffix')
    total_items=get_total_items(request_url,auth)
    
    if total_items >500:
        operations=create_operations_array(baseUrl,'GET',mapper,total_items)
        pool=Pool(4)
        data=pool.map(lambda operation: send_request_to_api(baseUrl,auth,operation),operations)
        pool.close()
        pool.join()
        
        data=[y for x in data for y in x]
    
    else:
        mapper['params']['count']=total_items
        data=r.get(request_url,auth=auth,params=mapper['params']).json().get(mapper['list_name'])
    
    return data