# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 15:49:22 2021

@author: Jiaming.Zhou

Reminder:
To use this script, please read the instruction in green and follow the instruction 
in gray starting with 'step *'. The comments of the code are also provided. 

To read the data in Oracle SQL, cx_Oracle package is required. To successfully 
run the package, please read the instruction in './Code/read me_cx_Oracle installation.txt

To use this script,the 'bsi-export_run-per-row.csv' and 'ft-export_run-per-row.csv' 
files on 'http://10.20.199.186:8084/' needs to be downloaded everytime and stored 
in './Data Summary'


Introduction:
This python script download the aims to calculate the production volume of each station per day
and per shift.
"""
import os
import pandas as pd
import numpy as np
import datetime
import urllib 
import urllib.request
path = "C:/JZhou/L6" 
#Step 0: For other users, please change the path to where you want to put files
#at the first time you run this script. No need to change it in the future.
os.chdir(path)


temp_path = path+'/Data Summary'
url = 'http://10.20.199.186:8083/cb/ft/ft-export_run-per-row.csv'  
urllib.request.urlretrieve(url, os.path.join(temp_path,'ft-export_run-per-row.csv'))
url = 'http://10.20.199.186:8084/cb/bsi/bsi-export_run-per-row.csv'  
urllib.request.urlretrieve(url, os.path.join(temp_path,'bsi-export_run-per-row.csv'))


from Code.daily_volume_collection import Daily_volume
from Code.volume_calculation import throughput

#Download daily volume from oracle sql.

start_date = "'2021-4-09'"#Step 1: Change the start date
end_date = "'2021-4-10'"#Step 2: Change the end date

volume = Daily_volume(start_date, end_date)

start_date1 = '2021-4-09'#Step 3: Change the start date
end_date1 = '2021-4-10'#Step 4: Change the end date. No need to be the next date.
bsi_path = './Data Summary/bsi-export_run-per-row.csv'
ft_path = './Data Summary/ft-export_run-per-row.csv'
savepath = f'./Data Summary/volume summary/volume_till_{end_date1}.csv'
#savepath = f'C:/Users/Jiaming.Zhou/Desktop/volume_from_{start_date1}_till_{end_date1}.csv'

throughput(volume, bsi_path, ft_path, savepath, start_date1, end_date1)








