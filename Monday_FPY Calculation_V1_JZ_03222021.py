# -*- coding: utf-8 -*-
"""
Created on Mon Mar 22 14:02:46 2021

@author: Jiaming.Zhou

Reminder:
To use this script, please read the instruction in green and follow the instruction 
in gray starting with 'step *'. The comments of the code are also provided. 

To read the data in Oracle SQL, cx_Oracle package is required. To successfully 
run the package, please read the instruction in './Code/read me_cx_Oracle installation.txt'

Introduction:
This python script mainly focused on weekly FPY calculation to help engineers find the
anomolies happened last week. FPY stands for first pass yield which represents the 
percentage of the boards that pass the test at the first time without any repair or rework.

The information from six stations(BOT AOI, TOP AOI, VI, OST, ICT, Pack AOI)
will be collected in this script. The pass/failed board information will be
saved in this process. The daily/weekly FPY of each station will be caluculated
and be saved as the results. The defects of the failure borads from each station
will also be saved. The ICT defects will be adjusted due to the loss of location 
in the raw data.

In this script, there will be eight parts. The first part will generate folders
to save the following data and results, define the date you want to calculate FPY. 
For the next six parts,in each part there are product pass/fail information 
collection, data preprocessing,and fpy calculation for one station. The last part will
be organize the FPY data in a new format and save it in 'Customer folder'

File folder structure:
1. 'WKxx' folder;
1. Under 'WKxx' folder, there will be a folder for each station(BOT AOI, TOP AOI, VI, OST, ICT, Pack AOI);
3. Under each folder, there will be a folder called "Fpy"; 
4. Under Fpy folder, there will be a folder called "Daily";
5. Under 'WKxx' folder, there will be a folder named "Customer"; 
6. Under the Customer folder, there will be two folders named "Defects" and "Fpy"

Work process for each station(All the following process can be done by running 
                              this python script EXCEPT ICT): 

1. Run a SQL Script. The return is a table of boards that first passed 
the station. Save the return dataframe into csv'{Station}_PASS.csv' under each
station's folder.(in Code/pass_fail_collection.py)
2. Run another SQL Script. The return is a table of boards that first failed 
the station. Save the return dataframe into csv'{Station}_FAILED.csv' under each
station's folder.(in Code/pass_fail_collection.py)
3. Run the "calculate_fpy_daily" function which will return daily FPY csv under
the "Daily" folder.
4. Run "total" function which will return the weekly FPY csv for each station under
"./Customer/Fpy"

"""

import os
import pandas as pd
import numpy as np
import datetime
path = "C:/JZhou/L6" 
#Step 0: For other users, please change the path to where you want to put files
#at the first time you run this script. No need to change it in the future.
os.chdir(path)

"""
Create folders and define the date to calculate FPY

To set the end date, it should be the target date plus one. For example, if you 
want to choose the data from 3.13-3.19, then the start date should be 3.13, the 
end date should be 3.20
"""

from Code.create_folder import create_folder

week = 'WK14'#Step 1: Change the week number
start_date = "'04/05/2021'"#Step 2: Change the start date
end_date = "'04/11/2021'"#Step 3: Change the end date.The date should be the next day of the end date you want.
end_date1 = "'04/10/2021'"#Step 3: Change the end date.The date is the real end date you want. No need to be next date.
week_date = pd.date_range(start_date, end_date1)  

create_folder(week)

"""
Collect PASS/FAIL Data and Calculate FPY

In this phase of code, the data collection function for each station is unique,
the {station}_PASS/{station}_FAIL functions are used to collect pass/fail data 
of the station. Calculate_fpy_daily function is used to calculate daily FPY for 
each station.
"""
from Code.pass_fail_collection import BOT_AOI_PASS
from Code.pass_fail_collection import BOT_AOI_FAIL
from Code.pass_fail_collection import TOP_AOI_PASS
from Code.pass_fail_collection import TOP_AOI_FAIL
from Code.pass_fail_collection import VI_PASS
from Code.pass_fail_collection import VI_FAIL
from Code.pass_fail_collection import OST_PASS
from Code.pass_fail_collection import OST_FAIL
from Code.pass_fail_collection import ICT_PASS
from Code.pass_fail_collection import ICT_FAIL
from Code.pass_fail_collection import PACK_AOI_PASS
from Code.pass_fail_collection import PACK_AOI_FAIL
from Code.calculate_daily_fpy import calculate_fpy_daily

#BOT AOI Station
station = 'BOT_AOI'
bot_aoi_pass_data = BOT_AOI_PASS(start_date,end_date)
bot_aoi_fail_data = BOT_AOI_FAIL(start_date,end_date)

savepath = f'{path}/{week}/{week}/{station}/Fpy/Daily/'# The path of the station file

for d in week_date:
    percent_df = calculate_fpy_daily(bot_aoi_pass_data, bot_aoi_fail_data, d,station)
    filename = savepath + str(d.strftime("%Y-%m-%d")) + '_fpy.csv'
    percent_df.to_csv(filename, index = False)


#TOP AOI Station    
station = 'TOP_AOI'
top_aoi_pass_data = TOP_AOI_PASS(start_date,end_date)
top_aoi_fail_data = TOP_AOI_FAIL(start_date,end_date)

savepath = f'{path}/{week}/{week}/{station}/Fpy/Daily/'

for d in week_date:
    percent_df = calculate_fpy_daily(top_aoi_pass_data, top_aoi_fail_data, d,station)
    filename = savepath + str(d.strftime("%Y-%m-%d")) + '_fpy.csv'
    percent_df.to_csv(filename, index = False)


#VI Station 
station = 'VI'
VI_pass_data = VI_PASS(start_date,end_date)
VI_fail_data = VI_FAIL(start_date,end_date)

savepath = f'{path}/{week}/{week}/{station}/Fpy/Daily/'

for d in week_date:
    percent_df = calculate_fpy_daily(VI_pass_data, VI_fail_data, d,station)
    filename = savepath + str(d.strftime("%Y-%m-%d")) + '_fpy.csv'
    percent_df.to_csv(filename, index = False)
    

#OST Station 
station = 'OST'
OST_pass_data = OST_PASS(start_date,end_date)
OST_fail_data = OST_FAIL(start_date,end_date)

savepath = f'{path}/{week}/{week}/{station}/Fpy/Daily/'

for d in week_date:
    percent_df = calculate_fpy_daily(OST_pass_data, OST_fail_data, d,station)
    filename = savepath + str(d.strftime("%Y-%m-%d")) + '_fpy.csv'
    percent_df.to_csv(filename, index = False)
    
    
#ICT Station 
station = 'ICT'
ICT_pass_data = ICT_PASS(start_date,end_date)
ICT_fail_data = ICT_FAIL(start_date,end_date)

savepath = f'{path}/{week}/{week}/{station}/Fpy/Daily/'

for d in week_date:
    percent_df = calculate_fpy_daily(ICT_pass_data, ICT_fail_data, d,station)
    filename = savepath + str(d.strftime("%Y-%m-%d")) + '_fpy.csv'
    percent_df.to_csv(filename, index = False)
    
    
#PACK AOI Station 
station = 'PACK_AOI'
PACK_AOI_pass_data = PACK_AOI_PASS(start_date,end_date)
PACK_AOI_fail_data = PACK_AOI_FAIL(start_date,end_date)

savepath = f'{path}/{week}/{week}/{station}/Fpy/Daily/'

for d in week_date:
    percent_df = calculate_fpy_daily(PACK_AOI_pass_data, PACK_AOI_fail_data, d,station)
    filename = savepath + str(d.strftime("%Y-%m-%d")) + '_fpy.csv'
    percent_df.to_csv(filename, index = False)


'''
FPY Transfer

In this part, the FPY calculated in the previous part will be organized into a
new format.
'''
from Code.calculate_daily_fpy import division    
from Code.calculate_daily_fpy import total
col_names = ['Date', 'Pass', 'Fail', 'FPY']
station_list = ['BOT_AOI', 'TOP_AOI', 'PACK_AOI', 'ICT', 'OST', 'VI']
fpy_list = []
for station in station_list:  
    
    dailyfpy_path = f'{path}/{week}/{week}/{station}/Fpy/Daily/'
    savepath1 =  f'{path}/{week}/{week}/Customer/Fpy/{week}_{station}_fpy.csv'
    
    percent_df = total(dailyfpy_path, col_names)
    fpy_list.append(percent_df['TOTAL']['FPY'])
    percent_df.to_csv(savepath1)

# Generate a station:fpy dictionary
Dict = dict(zip(station_list,fpy_list))
print("zip way:",Dict)