# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 13:59:38 2021

@author: Jiaming.Zhou

  Program Summary
  Purpose : Find the board that has been tested in FT station for several times
  Inputs : ft-export_run-per-row.csv
  Outputs : The boards that has been tested in FT station for more than one time
            with its total trial, second test time/result, last test time/result.
  Side Effects : 
"""

import os 
import pandas as pd
import numpy as np
import urllib 
import urllib.request
path = "C:/JZhou/L6" 
#Step 0: For other users, please change the path to where you want to put files
#at the first time you run this script. No need to change it in the future.
os.chdir(path)

# Download 'ft-export_run-per-row.csv' file under 'Data Summary' folder.
temp_path = path+'/Data Summary'
url = 'http://10.20.199.186:8083/cb/ft/ft-export_run-per-row.csv'  
ft_path = temp_path + '/ft-export_run-per-row.csv'
urllib.request.urlretrieve(url, ft_path)


ft = pd.read_csv(os.path.join(temp_path,'ft-export_run-per-row.csv'))
ft['FT Run Time'] = pd.to_datetime(ft['FT Run Time'])# Change the time data type

start_date = '1/1/2021'#step 1: Change the start date you want
end_date = '3/20/2021'#step 2: Change the end date you want
end_date1 = '3-20'#step 3: Change the end date you want
# Select the boards tested between the time period and are not golden board
ft_date = ft[(ft['FT Run Time']>= start_date)&(ft['FT Run Time']<= end_date)&(ft['Golden Board?'] != True)]
# Choose the product for Amazon
ft_data = ft_date[ft_date['Board Serial Number'].str.startswith('W')]

# Count the boards occurrence time and return the S/N with occurence >= 2
duplicate_sn = ft_data['Board Serial Number'].value_counts()# Count occurence time
duplicate_sn = duplicate_sn.reset_index()
duplicate_sn.columns = ["Board Serial Number","Occur number"]
duplicate_sn = duplicate_sn[duplicate_sn["Occur number"]>=2]

# Choose the data based on the S/N in duplicate_sn and sort by testing time
duplicate_ft = ft_data[ft_data['Board Serial Number'].isin(duplicate_sn["Board Serial Number"])]
duplicate_ft = duplicate_ft.sort_values(by = ['Board Serial Number','FT Run SortableTime'],ascending = (True,True))

#Select the last time and the second time based on the occurence sequence
last_time = list()
last_status = list()
second_time = list()
second_status = list()
for i in range(duplicate_sn.shape[0]):
    temp = duplicate_ft[duplicate_ft['Board Serial Number'] == duplicate_sn['Board Serial Number'][i]] # For each S/N, generate a temporary table
    temp = temp.reset_index(drop = True)
    last_time.append(temp['FT Run SortableTime'][duplicate_sn["Occur number"][i]-1])# Find last time
    last_status.append(temp['FT Run Result'][duplicate_sn["Occur number"][i]-1])# Find last result
    second_time.append(temp['FT Run SortableTime'][1])# Find second time
    second_status.append(temp['FT Run Result'][1])# Find second result
last_time = pd.DataFrame(last_time)# Change into df
last_status = pd.DataFrame(last_status)
second_time = pd.DataFrame(second_time)
second_status = pd.DataFrame(second_status)
duplicate_sn = pd.concat([duplicate_sn,second_time,second_status,last_time,last_status],axis= 1)#Combine
duplicate_sn.columns = ["Board Serial Number","Total Trials","Second Run SortableTime","Second Status","Last Run SortableTime","Last Status"]#Set name

filename = temp_path + f'/Bone Pile Repair/ft_total_record_till {end_date1}.csv'
duplicate_sn.to_csv(filename,index = False)#Save
