# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 15:49:22 2021

@author: Jiaming.Zhou

Reminder:
To use this script, please read the instruction in green and follow the instruction 
in gray starting with 'step *'. The comments of the code are also provided. 

To read the data in Oracle SQL, cx_Oracle package is required. To successfully 
run the package, please read the instruction in './Code/read me_cx_Oracle installation.txt

To use this script,the 'bsi-export_run-per-row.csv' & 'ft-export_run-per-row.csv' 
files on 'http://10.20.199.186:8084/' needs to be downloaded everytime and stored 
in './Data Summary' (4-13-2021: this has been done below.)

Introduction: 
This python script download the aims to calculate the production volume of each station per day
and per shift.

Modified in April 2021 @Lucy
"""

import os
import urllib.request
import urllib
import datetime as date
import numpy as np
import pandas as pd


def Execute():
    path = r"C:\Users\Lucy.Le\OneDrive - Foxconn Industrial Internet in North America\BI\L6"
    # Step 0: For other users, please change the path to where you want to put files
    # at the first time you run this script. No need to change it in the future.
    os.chdir(path)

    from Code.daily_volume_collection import Daily_volume
    from Code.volume_calculation import throughput

    temp_path = path+'/Data Summary'
    url = 'http://10.20.199.186:8083/cb/ft/ft-export_run-per-row.csv'
    # temp = "http://10.20.199.186:8082/cb/ft/ft-export_run-per-row.csv"
    urllib.request.urlretrieve(url, os.path.join(
        temp_path, 'ft-export_run-per-row.csv'))
    url = 'http://10.20.199.186:8084/cb/bsi/bsi-export_run-per-row.csv'
    # temp ="http://10.20.199.186:8083/cb/bsi/bsi-export_run-per-row.csv"

    urllib.request.urlretrieve(url, os.path.join(
        temp_path, 'bsi-export_run-per-row.csv'))

    # Download daily volume from oracle sql.

    # ***** If need to manually input *****
    # Step 1: Change the start date
    start_date = "'2021-4-21'"
    # Step 2: Change the end date to real end date
    end_date = "'2021-4-21'"
    start_date1 = '2021-4-21'
    end_date1 = '2021-4-21'

    start_dt2 = date.datetime(2021, 4, 21, 0, 0, 0)
    end_dt2 = date.datetime(2021, 4, 21, 23, 59, 59)

    # ***** If need to manually input *****
    volume = Daily_volume(start_date, end_date)

    bsi_path = './Data Summary/bsi-export_run-per-row.csv'
    ft_path = './Data Summary/ft-export_run-per-row.csv'
    savepath = f'./Data Summary/volume summary/volume_till_{end_date1}.csv'

    throughput(volume, bsi_path, ft_path, savepath,
               start_date1, end_date1, start_dt2, end_dt2)

    print("Done 2")


Execute()

# = datetime.date(2021, 4, 14)  # Step 1: Change the start date
#start_date1 = o_start_date.strftime('%Y-%m-%d')
#start_date = start_date1
# if start_date[5] == '0':
#    start_date1 = start_date[:5] + start_date[6:]
#    start_date = "'" + start_date1 + "'"

#day_count = 7
# Step 2: Change the end date to real end date
#o_end_date = o_start_date + datetime.timedelta(days=day_count-1)
#end_date1 = o_end_date.strftime('%Y-%m-%d')
#end_date = end_date1
# if end_date[5] == '0':
#    end_date1 = end_date[:5] + end_date[6:]
#    end_date = "'" + end_date1 + "'"

# # ***** Test string input *****
# if start_date == "'2021-04-19'":
#     print(True)
# if start_date1 == '2021-4-19':
#     print(True)
# if end_date == "'2021-04-19'":
#     print(True)
# if end_date1 == '2021-4-19':
#     print(True)
