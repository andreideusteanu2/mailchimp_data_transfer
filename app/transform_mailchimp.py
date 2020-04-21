#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 12:55:36 2019

@author: developer
"""

from datetime import datetime
from bs4 import BeautifulSoup
from copy import deepcopy

from transform import datetime_parsers_with_timezone as dpwt

def transform_campaign_data(campaignDetails):
    thisCampaign={}
    thisCampaign['NoEmailsSent']=campaignDetails.get('emails_sent')
    thisCampaign['Id']=campaignDetails.get('id')
    thisCampaign['WebId']=campaignDetails.get('web_id')
    thisCampaign['ToListId']=campaignDetails.get('recipients').get('list_id')
    thisCampaign['ToList']=campaignDetails.get('recipients').get('list_name')
    thisCampaign['From']=campaignDetails.get('settings').get('from_name')
    thisCampaign['ReplyTo']=campaignDetails.get('settings').get('reply_to')
    thisCampaign['Subject']=campaignDetails.get('settings').get('subject_line')
    thisCampaign['Name']=campaignDetails.get('settings').get('title')
    thisCampaign['SentAt']=datetime.strftime(
                            dpwt.parse_utc_to_bucharestTz(
                                    datetime.strptime(campaignDetails.get('send_time').replace(':','')
                                                ,'%Y-%m-%dT%H%M%S%z')
                                    ),'%Y-%m-%d %H:%M:%S'
                                    )
    
    summary=campaignDetails.get('report_summary')  
    summary.pop('ecommerce',None)
    summary.pop('open_rate',None)
    summary.pop('click_rate',None)                                    
    for key in summary.keys():
        summary[key.title().replace("_",'')]=summary.pop(key) 
    
    thisCampaign['ReportSummary']=[summary]                        
    
    return thisCampaign


def transform_unsubscribes(campaignUnsubscribes):
    thisCampaignUnsubscribes=[]
    
    for unsubscribed in campaignUnsubscribes:
        out={}
        out['EmailAddress']=unsubscribed.get('email_address')
        out['EmailId']=unsubscribed.get('email_id')
        out['ListId']=unsubscribed.get('list_id')
        out['UnsubscribedAt']=unsubscribed.get('timestamp')
        
        thisCampaignUnsubscribes.append(out)
    
    return thisCampaignUnsubscribes


def transform_advices(campaignAdvices):

    thisCampaignAdvices=[]
    
    for adviced in campaignAdvices:
        out={}
        out['AdviceType']=adviced.get('type').replace('advice-','').title()
        soup = BeautifulSoup(adviced.get('message'),features="lxml")
        out['Advice']=soup.get_text()
        
        thisCampaignAdvices.append(out)
    
    return thisCampaignAdvices


def transform_domainPerformances(campaignDomainPerformances):
    
    thisCampaignDomainPerformances=[]
    
    for domainPerformance in campaignDomainPerformances:
        out=deepcopy(domainPerformance)
        out.pop('emails_pct',None)
        out.pop('opens_pct',None)
        out.pop('clicks_pct',None)
        out.pop('bounces_pct',None)
        out.pop('unsubs_pct',None)
        for key in out.keys():
            out[key.title().replace("_",'')]=out.pop(key)
        thisCampaignDomainPerformances.append(out)
        
    
    return thisCampaignDomainPerformances


def remove_colon_from_tz_offset(dateTimeStr):
    inter=dateTimeStr.split(':')
    last=inter.pop(-1)
    out=':'.join(inter)+last
    return out


def transform_activity_details(activityDetails):
    for detail in activityDetails:
        detail.pop("ip",None)
        if "+" in detail.get('timestamp') and "T" in detail.get('timestamp'):
            out=remove_colon_from_tz_offset(detail.get('timestamp'))
            detail['timestamp']=datetime.strftime(
                            dpwt.parse_utc_to_bucharestTz(
                                    datetime.strptime(out,'%Y-%m-%dT%H:%M:%S%z')
                                                        )
                                                ,'%Y-%m-%d %H:%M:%S')
        else:
            detail['timestamp']=None
        
        if 'type' not in detail.keys():
            detail['type']=None
        if 'url' not in detail.keys():
            detail['url']=None
        for key in detail.keys():
            detail[key.title()] = detail.pop(key)
        

def transform_activities(campaignActivities):
    thisCampaignActivity=[]
    
    for activity in campaignActivities:
        out={}
        out['EmailAddress']=activity.get('email_address')
        out['EmailId']=activity.get('email_id')
        activityDetails=activity.get('activity')
        if activityDetails:
            if len(activityDetails)>0:
                transform_activity_details(activityDetails)
        else:
            activityDetails=None
        
        out['Activity']=activityDetails
        
        thisCampaignActivity.append(out)
    
    return thisCampaignActivity


def transform_unsub_to_activity(thisCampaignUnsubscribes):
    thisCampaignUnsubActivity=[]
    for unsub in thisCampaignUnsubscribes:
        actUnsub={}
        actUnsub['EmailAddress']=unsub.get('EmailAddress')
        actUnsub['EmailId']=unsub.get('EmailId')
        
        actUnsubDetail={}
        actUnsubDetail['Type']=None
        actUnsubDetail['Action']='unsubscribe'
        actUnsubDetail['Timestamp']=unsub.get('UnsubscribedAt')
        actUnsub['Activity']=[actUnsubDetail]
        thisCampaignUnsubActivity.append(actUnsub)
        
    return thisCampaignUnsubActivity


def transform_json_to_single_list(mapper,output):
    data=[]
    for element in output:
        data.extend(element.get(mapper.get('list_name')))
        
    return data