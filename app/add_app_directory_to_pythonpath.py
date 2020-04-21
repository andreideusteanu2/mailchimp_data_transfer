#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 17:40:49 2018

@author: developer
"""
import os
from pathlib import Path
from sys import path

def main():
    scriptpath = Path(os.path.realpath(__file__))
    #.parent property = the parent directory of the path
    runningFolder=(scriptpath.parent)
    
    print(str(runningFolder))
    
    if str(runningFolder) not in path:
        minLen=999999
        for index,directory in enumerate(path):
            if 'site-packages' in directory and len(directory)<=minLen:
                minLen=len(directory)
                sitePackagesIndex=index
                
        pathSitePckgs=Path(path[sitePackagesIndex])
        with open(str(pathSitePckgs/'current_machine_paths.pth'),'w') as pth_file:
            pth_file.write(str(runningFolder))
        
if __name__=='__main__':
    main()
