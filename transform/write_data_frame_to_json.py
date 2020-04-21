#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 18:54:16 2019

@author: developer
"""
import pandas as pd 
from pathlib import Path
from json import dumps

def none_to_empty_string(s):
    return str(s or '')    

def convert_dataframe_columns_to_string(dataOut) -> pd.DataFrame:
    from datetime import datetime
    from pandas import isnull
    #replace nan to None because nan goes literally in json
    #whereas None goes as empty and can be imported as null
    #credits to https://stackoverflow.com/questions/14162723/replacing-pandas-or-numpy-nan-with-a-none-to-use-with-mysqldb/54403705
    dataFormated=dataOut.where(pd.notnull(dataOut), None)
    dataFormated=dataOut.replace({pd.np.nan: None})
    
    #converting columns to a string format
    dataFormated=dataFormated.apply(lambda col: col.apply(lambda x: str(x) if not isnull(x) else x)  if col.dtype in ['int64','float64','bool','Int64']
                else col.astype(object)
                        .where(col.notnull(),None)
                        .apply(lambda x: datetime.strftime(x,'%Y-%m-%d %H:%M:%S') if not isnull(x) else x)                     
                        if col.dtype in ['datetime64[ns]'] and col.name!='Date'
                else col.apply(lambda x: datetime.strftime(x,'%Y-%m-%d')) 
                        if col.dtype=='datetime64[ns]' and col.name=='Date'
                else col.where(pd.notnull(col),None).apply(none_to_empty_string)
                )
    
    return dataFormated
    
def main(dataIn:pd.DataFrame,filePath:Path,inputType='DataFrame',require_string_transformation=False):
    if inputType=='DataFrame':
        dataOut=dataIn.copy()
        if require_string_transformation:
            dataOut=convert_dataframe_columns_to_string(dataOut)
        dataToWrite=dataOut.to_dict(orient='records')

    if inputType=='list':
        dataToWrite=dataIn
    with open(str(filePath),'w') as f:
        for line in dataToWrite:
            f.write(dumps(line))
            f.write('\n')