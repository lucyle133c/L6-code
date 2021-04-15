# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 10:29:30 2021

@author: Jiaming.Zhou

  Program Summary
  Purpose : Find out the boards that are continuously staying in the repair station
  Inputs : ft,bsi,ict data, inbound data
  Outputs : A list of the boards that are in the repair station and its duration time
            A list of the boards that are in the repair station, its duration time and its issue
  Side Effects :
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


temp_path = path+'/Data Summary/Bone Pile Repair'
url = 'http://10.20.199.186:8083/cb/ft/ft-export_run-per-row.csv'  
urllib.request.urlretrieve(url, os.path.join(temp_path,'ft-export_run-per-row.csv'))
url = 'http://10.20.199.186:8084/cb/bsi/bsi-export_run-per-row.csv'  
urllib.request.urlretrieve(url, os.path.join(temp_path,'bsi-export_run-per-row.csv'))
url = 'http://10.20.199.186:8082/cb/ict/ict-export_run-per-row.csv'  
urllib.request.urlretrieve(url, os.path.join(temp_path,'ict-export_run-per-row.csv'))

start_date = "'03-29-2021'"#Step 1: Change the start date
end_date = "'04-04-2021'"#Step 2: Change the end date
end_date1 = '3-28'#Step 3: Change the end date

from Code.bone_pile import bone_pile1
from Code.bone_pile import bone_pile2
from Code.bone_pile import bone_repair

#Collect two raw data
data1 = bone_pile1(start_date,end_date)
data2 = bone_pile2(start_date,end_date)

#Combine
data = pd.concat([data1,data2])
data = data.reset_index(drop = True)
data.to_csv(os.path.join(temp_path,f'defects_all from {start_date} till {end_date}.csv'))


repair_path =  path+f'/Data Summary/Bone Pile Repair/inbound till {end_date1}.xlsx'
bsi_path = path+'/Data Summary/Bone Pile Repair/bsi-export_run-per-row.csv'
ft_path = path+'/Data Summary/Bone Pile Repair/ft-export_run-per-row.csv'
ict_path = path+'/Data Summary/Bone Pile Repair/ict-export_run-per-row.csv'
savepath1 = path+f'/Data Summary/Bone Pile Repair/Bone Pile Repair till {end_date1}.csv'
savepath2 = path+f'/Data Summary/Bone Pile Repair/Bone Pile Repair till {end_date1}_location.csv'

bone_repair(repair_path, bsi_path, ft_path, ict_path, data, savepath1, savepath2)