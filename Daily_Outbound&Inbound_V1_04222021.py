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

Modified in April 2021 @Lucy
"""

import datetime
import os
import pandas as pd
import numpy as np


def Execute():
    path = r"C:\Users\Lucy.Le\OneDrive - Foxconn Industrial Internet in North America\BI\L6"
    # Step 0: For other users, please change the path to where you want to put files
    # at the first time you run this script. No need to change it in the future.
    os.chdir(path)

    from Code.outbound import inbound_outbound
    from Code.outbound import OUTBOUND_COLLECTION

    '''
    Part1: Read and download the outbound data. Combine with the inbound data from 
    pangus and outout a repair summary file.
    '''

    # *** If need to manually input ***
    # # Step 1: Change the start date
    start_date = "'2021/04/21'"
    # # Step 2: Change the end date to the next day of the end date you want.
    end_date = "'2021/04/22'"
    # # Step 3: Type in the real end date in another format, not next date
    end_date1 = '4-21'

    outbound_data = OUTBOUND_COLLECTION(start_date, end_date)
    inbound_path = f'./Data Summary/inbound till {end_date1}.xlsx'
    savepath = f'./Data Summary/inbound_outbound summary/Repair Summary till {end_date1}.csv'

    outbound_inbound_result = inbound_outbound(
        inbound_path, outbound_data, savepath)

    print("Done 1")


Execute()


# ******** Practice Code ********
# # Step 0: Download inbound file from pangus
# # Step 1: Change the start date
# o_start_date = datetime.date(2021, 1, 4)
# start_date = o_start_date.strftime('%Y/%m/%d')
# start_date = "'" + start_date + "'"

# # Step 2: Change the end date to the next day of the end date you want.
# day_count = 7  # If more than 1 day, change count to amount of days
# o_end_date = o_start_date + datetime.timedelta(days=day_count)
# end_date = "'" + o_end_date.strftime('%Y/%m/%d') + "'"

# # Step 3: Type in the real end date in another format, not next date
# o_end_date = o_start_date + datetime.timedelta(days=day_count-1)
# end_date1 = o_end_date.strftime('%m-%d')
# if end_date1[0] == '0':
#     end_date1 = end_date1[1:]


# # *** Test string input ***
# if start_date == "'2021/04/14'":  # start_date = "'2021/04/19'"
#     print(True)
# if end_date == "'2021/04/20'":  # end_date = "'2021/04/20'"
#     print(True)
# if end_date1 == '4-19':  # end_date1 = '4-19
#     print(True)
