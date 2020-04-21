#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 21:45:19 2020

@author: developer
"""
import pandas as pd
from datetime import datetime

def none_to_empty_string(s):
    return str(s or '')  

def main(data:pd.DataFrame) -> pd.DataFrame:
    
    #replace nan to None because nan goes literally in json
    #whereas None goes as empty and can be imported as null
    dataFormated=data.replace({pd.np.nan: None})
    dataFormated=data.where((pd.notnull(data)), None)
    
    #converting columns to a string format
    dataFormated=dataFormated.apply(lambda col: col.apply(lambda x: str(x) if not pd.isnull(x) else x)  if col.dtype in ['int64','float64','bool']
                else col.astype(object)
                        .where(col.notnull(),None)
                        .apply(lambda x: datetime.strftime(x,'%Y-%m-%d %H:%M:%S') if not pd.isnull(x) else x)                     
                        if 'datetime64[ns' in str(col.dtype) and col.name!='Date'
                else col.apply(lambda x: datetime.strftime(x,'%Y-%m-%d')) 
                        if str(col.dtype) in ['datetime64[ns]','object'] and col.name=='Date'
                else col.where(pd.notnull(col),None).apply(none_to_empty_string)
                )
    
    return dataFormated