#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 18:54:16 2019

@author: developer
"""
import pandas as pd 
from pathlib import Path
from transform import convert_dataframe_columns_to_strings as df_to_str
    
def main(dataIn:pd.DataFrame,filePath:Path,require_string_transformation=True):
    dataOut=dataIn.copy()
    if require_string_transformation:
        dataOut=df_to_str.main(dataOut)
    
    dataOut.to_json(str(filePath)
                    ,orient='records'
                    ,lines=True)