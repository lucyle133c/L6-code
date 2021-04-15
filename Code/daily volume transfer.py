# -*- coding: utf-8 -*-
"""
Created on Thu Apr  1 12:45:18 2021

@author: Jiaming.Zhou

  Program Summary
  Purpose : Transfer the daily volume data in a different structure
  Inputs : Original daily volume data
  Outputs : Daily volume data in new format
  Side Effects :

"""

import os
import pandas as pd
import numpy as np
path = r"C:\JZhou\L6\Data Summary\volume summary"
#Step 0: For other users, please change the path to where you want to put files
#at the first time you run this script. No need to change it in the future.
os.chdir(path)

data = pd.read_csv(r'volume_from_2021-1-04_till_2021-3-31.csv')# Read original data
shift = ['1st Shift','2nd Shift','3rd Shift']
date = np.unique(data['Date'])# Get existing date
station_list = ["BOT", "TOP", "VI", "OST", "ICT", "PACK","BSI","FT1","FT2"]
data_temp = pd.DataFrame()
for t in date:
    data_temp1 = pd.DataFrame()
    for s in shift:
        data1 = data[data['Shift'] == s]
        data1 = data1[data1['Date'] == t]

        data2 = data1.loc[:,station_list].T
        data2 = data2.reset_index()
        data2.columns = ['Station','# bad boards','# good boards','Total boards','Station FPY']
        data2['date'] = t
        data2['shift'] = s
        data2['shift FPY'] = list(np.cumprod(data2['Station FPY']))[-1]
        data_temp1 = pd.concat([data_temp1,data2])
    
    data_temp1['Daily FPY'] = list(np.cumprod(np.unique(data_temp1['shift FPY'])))[-1]
    data_temp = pd.concat([data_temp,data_temp1])
data_temp = data_temp[['date','shift','Station','# bad boards','# good boards','Total boards','Station FPY','shift FPY','Daily FPY']]        
filename = path + '\DailyVolumeTransfer_1.4.2021-3.31.2021.csv'
data_temp.to_csv(filename)
