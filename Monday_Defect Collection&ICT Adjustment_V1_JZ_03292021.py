# -*- coding: utf-8 -*-
"""
Created on Mon Mar 29 08:55:53 2021

@author: Jiaming.Zhou

Reminder:
To use this script, please read the instruction in green and follow the instruction 
in gray starting with 'step *'. The comments of the code are also provided. 

To read the data in Oracle SQL, cx_Oracle package is required. To successfully 
run the package, please read the instruction in './Code/read me_cx_Oracle installation.txt

To use this script,the 'ict-export_run-per-row.csv' file on 'http://10.20.199.186:8084/'
needs to be downloaded everytime and stored in './ICT Web Scrapping'


Introduction:
This python script aims to 
1. Run the first SQL Script. The return is a table of defects. Save the return 
dataframe into csv'{Station}_{Week}.csv' under the "Defects" folder;
2. Due to ICT defect miss the location information. We are going to use the 
'ict-export_run-per-row.csv' file on 'http://10.20.199.186:8084/' to get the info
and add the location indo into the ICT Defect file. 
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

temp_path = path+'/ICT Web Scrapping'
url = 'http://10.20.199.186:8082/cb/ict/ict-export_run-per-row.csv'  
urllib.request.urlretrieve(url, os.path.join(temp_path,'ict-export_run-per-row.csv'))


from Code.defect_collection import BOT_AOI_Defect
from Code.defect_collection import TOP_AOI_Defect
from Code.defect_collection import VI_Defect
from Code.defect_collection import ICT_Defect
from Code.defect_collection import OST_Defect
from Code.defect_collection import PACK_AOI_Defect

week = 'WK14'#Step 1: Change the week number
week1 = "'WK14'"#Step 2: Change the week number
start_date = "'04/05/2021'"#Step 3: Change the start date
end_date = "'04/11/2021'"#Step 4: Change the end date.The date should be the next day of the end date you want.
save_path = f"./{week}/{week}/Customer/Defects"


'''
First part:
Download and save the defect information of BOT_AOI, TOP_AOI, VI, OST, ICT
'''
BOT_AOI_Defect_Data = BOT_AOI_Defect(start_date,end_date,week1)
BOT_AOI_Defect_Data.to_csv(os.path.join(save_path, f"BOT_AOI_{week}.csv"), index = False)
TOP_AOI_Defect_Data = TOP_AOI_Defect(start_date,end_date,week1)
TOP_AOI_Defect_Data.to_csv(os.path.join(save_path, f"TOP_AOI_{week}.csv"), index = False)
VI_Defect_Data = VI_Defect(start_date,end_date,week1)
VI_Defect_Data.to_csv(os.path.join(save_path, f"VI_{week}.csv"), index = False)
OST_Defect_Data = OST_Defect(start_date,end_date,week1)
OST_Defect_Data.to_csv(os.path.join(save_path, f"OST_{week}.csv"), index = False)
PACK_AOI_Defect_Data = PACK_AOI_Defect(start_date,end_date,week1)
PACK_AOI_Defect_Data.to_csv(os.path.join(save_path, f"PACK_AOI_{week}.csv"), index = False)




'''
Second part:
Download and save the defect information of ICT and update the location info
'''
from Code.ICT_defect_correction import ict_defect

ICT_Defect_Data = ICT_Defect(start_date,end_date,week1)
ICT_Defect_Data.to_csv(os.path.join(save_path, f"ICT_{week}.csv"), index = False)

data_path = './ICT Web Scrapping/ict-export_run-per-row.csv'
defect_df = ict_defect(data_path, ICT_Defect_Data)
defect_df.to_csv(f'./{week}/{week}/Customer/Defects/ICT_{week}.csv', index = False)