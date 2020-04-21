#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  3 15:40:08 2019

@author: developer
"""

'''
logic
0. authenticate to the api with the api key
1. get all time campaigns as list
2. for each campaign
2.1 get sent to
2.2 get unsubscribers
2.3 get campaign open reports
2.4 get campaign abuse reports
3. evaluate the data format of the reports
4. transform data into a campaign, activity, email structure
5. create marketing dataset in gbq
6. upload data to gbq into a table
'''

from multiprocess import Pool
from copy import deepcopy
from sys import path
import pandas as pd

import determine_running_folder_path as drfp

from transform import write_data_frame_to_json as wdftj

from load import load_from_local_to_gbq as load

pathFiles=drfp.main()
path.append(str(pathFiles/'mailchimp_data_transfer'))
baseUrl='https://us1.api.mailchimp.com/3.0/'

import extract_mailchimp as emi
import transform_mailchimp as tmi

#batching
#create the operations array
#if total_items >1000
#use the logic from get_all_data
#to add the operation to the batch
#post at /batches
#every 2 min check the response at /batches/{batch_id} -> total_operations vs finished operations
#A GET call to the URL returned in the batch resources response_body_url 
#returns a gzipped tar archive of JSON files that contain the results 
#of operations in the following format:
#read the results of the batch operation
#this should be the transform batches to total items

def set_mappers():

    mappers={'campaigns':{'params':
                                {'status':'sent'
                                ,'since_send_time':'2019-01-01T00:00:00+02:00'
                                ,'fields':['campaigns']
                                }
                            ,'request_suffix':'campaigns'
                            ,'list_name':'campaigns'
                            ,'transformer':tmi.transform_campaign_data
                            }
        ,'campaignActivity':{'params':{'fields':['emails']}
                            ,'request_suffix':'reports/[__campaign_id__]/email-activity'
                            ,'list_name':'emails'
                            ,'transformer':tmi.transform_activities
                            }
        ,'campaignUnsubscribed':{'params':{'fields':['unsubscribes']}
                            ,'request_suffix':'reports/[__campaign_id__]/unsubscribed'
                            ,'list_name':'unsubscribes'
                            ,'transformer':tmi.transform_unsubscribes
                            }
        ,'campaignAdvice':{'params':{'fields':['advice']}
                            ,'request_suffix':'reports/[__campaign_id__]/advice'
                            ,'list_name':'advice'
                            ,'transformer':tmi.transform_advices
                            }
        ,'campaignDomainPerformance':{'params':{'fields':['domains']}
                                    ,'request_suffix':'reports/[__campaign_id__]/domain-performance'
                                    ,'list_name':'domains'
                                    ,'transformer':tmi.transform_domainPerformances
                                    }
        }
        
    return mappers

def get_campaign_data(campaign,mappers,mappersToProcess,auth):
    
    campaignData={}
    
    for mapperKey in mappersToProcess:
        print(mapperKey)
        mapperIn=mappers.get(mapperKey)
        mapper=emi.copy_set_mapper_with_campaign_id(campaign.get('Id'),mapperIn)
        rawData=emi.get_all_data(mapper,auth)
        campaignData[mapperKey]=mappers.get(mapperKey).get('transformer')(rawData)
        
    if campaignData.get('campaignUnsubscribed'):
        if len(campaignData.get('campaignUnsubscribed'))>0:
            thisCampaignUnsubActivity=tmi.transform_unsub_to_activity(campaignData.get('campaignUnsubscribed'))
            campaignData['campaignActivity'].extend(thisCampaignUnsubActivity)
        
    campaignData.pop('campaignUnsubscribed',None)
    
    return campaignData

def consolidate_campaign_activity_data(campaign,auth,mappers):
    print("Working on campaign",campaign.get('Id'),'activity data')
    mappersToProcess=list(mappers.keys())
    
    mappersToProcess.remove('campaigns')
    mappersToProcess.remove('campaignAdvice')
    mappersToProcess.remove('campaignDomainPerformance')
    
    campaignData=get_campaign_data(campaign,mappers,mappersToProcess,auth)
    
    campaignEmailActivity={}
    campaignEmailActivity['CampaignId']=campaign.get('Id')
    campaignEmailActivity['ToListId']=campaign.get('ToListId')
    campaignEmailActivity['EmailActivity']=campaignData['campaignActivity']
        
    output={'filePath':pathFiles/'mailchimp_data_transfer'/'campaigns_activities'/(campaign.get('Id')+'_activity.json')
    ,'data':campaignEmailActivity}
    
    return output

def consolidate_campaign_data(campaign,auth,mappers):
    print("Working on campaign",campaign.get('Id'),'main data')
    
    mappersToProcess=list(mappers.keys())
    
    mappersToProcess.remove('campaigns')
    mappersToProcess.remove('campaignUnsubscribed')
    mappersToProcess.remove('campaignActivity')
    
    campaignData=get_campaign_data(campaign,mappers,mappersToProcess,auth)
    
    campaignOut=deepcopy(campaign)
    campaignOut['DomainPerformance']=campaignData['campaignDomainPerformance']
    campaignOut['Advice']=campaignData['campaignAdvice']
    
    return campaignOut


def get_relevant_campaigns(mappers):
    
    auth=emi.set_auth()
    campaignsRaw=emi.get_all_data(mappers['campaigns'],auth)
    campaigns=list(map(mappers['campaigns'].get('transformer'),campaignsRaw))
    
    poolCampaigns = Pool(4)
    campaignsToWrite=poolCampaigns.map(lambda campaign: consolidate_campaign_data(campaign,auth,mappers),campaigns)
    poolCampaigns.close() 
    poolCampaigns.join()
    
    return campaignsToWrite
    
    
def write_load_to_gbq(data,gcs_bucket_name,bq_dataset_id,bq_table_id,localFilePath):
    wdftj.main(data,localFilePath,inputType='list')
   
    load.main(file_path=localFilePath
          ,bucket_name=gcs_bucket_name
          ,dataset_id=bq_dataset_id
          ,table_id=bq_table_id
          )

def main():
    auth=emi.set_auth()
    mappers=set_mappers()
     
    campaigns=pd.read_csv(pathFiles/'mailchimp_data_transfer'/'campaignsProcessed.csv')
    campaigns=campaigns[['Id','ToListId','NoEmailsSent']]
    campaignsList=campaigns.to_dict(orient='records')
        
    for campaign in campaignsList:
        campaign_activity=consolidate_campaign_activity_data(campaign,auth,mappers)
        
        write_load_to_gbq(campaign_activity['data']
                        ,'marketing2prf'
                        ,'marketing'
                        ,'email_campaigns_activity'
                        ,campaign_activity['filePath'])
        
            
if __name__=="__main__":
    main()