#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 19:39:24 2018

@author: developer
"""

def main():
    import os
    from pathlib import Path
    scriptpath = Path(os.path.realpath(__file__))
    #.parent property = the parent directory of the path
    runningFolder=(scriptpath.parent)
    return runningFolder

if "__name__"=="__main__":
    main()