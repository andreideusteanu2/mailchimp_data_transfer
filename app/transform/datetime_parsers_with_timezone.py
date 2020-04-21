#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 15:06:56 2019

@author: developer
"""
from pandas import isnull, NaT
from datetime import datetime
import pytz
#bucharestTz=tz.tzlocal()
bucharestTz=pytz.timezone('Europe/Bucharest')
def parse_string_to_utc(string:str,timeFormat:str) -> datetime:
    return (pytz.UTC).localize(datetime.strptime(string,timeFormat))

def parse_utc_to_bucharestTz(datetimeObj:datetime) -> datetime:
    if isnull(datetimeObj):
        return NaT
    else:
        return datetimeObj.astimezone(bucharestTz)
    
def parse_bucharestTz_to_utc(datetimeObj:datetime) -> datetime:
    if isnull(datetimeObj):
        return NaT
    else:
        return datetimeObj.astimezone(pytz.UTC)

def parse_bucharestTzString_to_utc(string:str,timeFormat='%Y-%m-%d %H:%M:%S',stringType='datetime',returnType='datetime') -> datetime:
    stringTypes={'datetime','date','time'}
    returnTypes={'datetime','string'}
    if stringType not in stringTypes:
        ValueError("results: stringType must be one of %r." % stringTypes)
    else:
        if returnType not in returnTypes:
            ValueError("results: returnType must be one of %r." % returnTypes)
        else:
            out=(bucharestTz.localize(datetime.strptime(string,timeFormat))).astimezone(pytz.UTC)
            if returnType=='datetime':
                return out
            if returnType=='string':
                return datetime.strftime(out,'%Y-%m-%d %H:%M:%S')
        return
    
def localize_datetime_to_utc(datetimeObj:datetime):
    if isnull(datetimeObj):
        return NaT
    else:
        return (pytz.UTC).localize(datetimeObj)
    
def localize_datetime_to_bucharestTz(datetimeObj:datetime):
    if isnull(datetimeObj):
        return NaT
    else:
        return bucharestTz.localize(datetimeObj)
    
def transfer_datetime_from_bucharestTz_to_utc(datetimeObj:datetime):
    if isnull(datetimeObj):
        return NaT
    else:
        return parse_bucharestTz_to_utc(localize_datetime_to_bucharestTz(datetimeObj))
    
def parse_to_hour_only(datetimeObj):
    if isnull(datetimeObj):
        return NaT
    else:
        return datetimeObj.replace(minute=0,second=0,microsecond=0)
    