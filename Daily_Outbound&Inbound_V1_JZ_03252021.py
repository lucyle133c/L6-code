# -*- coding: utf-8 -*-
"""
Created on Thu Mar 25 08:16:07 2021

@author: Jiaming.Zhou


Reminder:
    
To use this script, please read the instruction in green and follow the instruction 
in gray starting with 'step *'. The comments of the code are also provided.    
    
To read the data in Oracle SQL, cx_Oracle package is required. To successfully 
run the package, please read the instruction in './Code/read me_cx_Oracle installation.txt'

To use this script, inbound fill need to be downloaded from pangus in advance.
In pangus system, search the table named 'To repair information query'. Set up 
the time range and click execute. Download the to_repair data from pangus and 
name it 'inbound till_{date}'(e.g. inbound_3-7.csv) under'./Data Summary'


Introduction:
This python script aims to calculate the daily inbound and outbound data. In the 
second part of the code. Bar charts will be drawn to help engineers directly see
the comparison. 
"""


import os
import pandas as pd
import numpy as np
path = "C:/JZhou/L6"
#Step 0: For other users, please change the path to where you want to put files
#at the first time you run this script. No need to change it in the future.
os.chdir(path)

from Code.Inbound_outbound import OUTBOUND_COLLECTION
from Code.Inbound_outbound import inbound_outbound
'''
Part1: Read and download the outbound data. Combine with the inbound data from 
pangus and outout a repair summary file.
'''

start_date = "'2021/04/05'"#Step 1: Change the start date
end_date = "'2021/04/11'"#Step 2: Change the end date. The date should be the next day of the end date you want.
end_date1 = '4-12'#Step 3: Type in the end date in another format. No need to be the next day
outbound_data = OUTBOUND_COLLECTION(start_date,end_date)
inbound_path = f'./Data Summary/inbound till {end_date1}.xlsx'
savepath = f'./Data Summary/inbound_outbound summary/Repair Summary till {end_date1}.csv'

outbound_inbound_result = inbound_outbound(inbound_path, outbound_data, savepath)



