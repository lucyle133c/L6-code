# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 08:08:04 2021

@author: Jiaming.Zhou

Reminder:
To read the data in Oracle SQL, cx_Oracle package is required. To successfully 
run the package, please read the instruction in './Code/read me_cx_Oracle installation.txt'

Introduction:
This python script aims to download the daily PASS/FAIL data. 
"""

import cx_Oracle 
import pandas as pd
import datetime

cx_Oracle.init_oracle_client(lib_dir=r"C:\JZhou\oracle sql\instantclient_19_10")#Change the direction to where you put the oracle instant client
#For PANGUS PRD account, ask Xiaolong MA for credential.
dsnStr = cx_Oracle.makedsn("10.20.193.53", "1521", "IMSDBPROD")#The structure is (ip,port,SID)
db = cx_Oracle.connect('xiaoma','foxconn123MKE',dsn=dsnStr)#The structure is (Username,Password,dsnstr)

cur = db.cursor() 

#For the next function, the SQL command is from Cora's script and the start_date
#and end_date will be valued while using the function.
def Daily_volume(start_date,end_date):
    command = f"SELECT LB_ID, WP_ID, IS_PASS, TO_CHAR(WP_CMP_DATE, 'MM/DD/YYYY HH24:MI:SS') WP_CMP_DATE\
                   FROM IMS.TB_PM_MO_LBWP\
                WHERE WP_ID IN ('Process009', 'Process019', 'PTH-VI', 'PTH-OST', 'PTH-ICT', 'PTH-Packing')\
                      AND LB_ID LIKE 'W%'\
                      AND TO_DATE(WP_CMP_DATE) BETWEEN TO_DATE({start_date}, 'YYYY-MM-DD') AND TO_DATE({end_date}, 'YYYY-MM-DD')"
    cur.execute(command) # Execute SQL command
    rows = cur.fetchall()
    volume_data = pd.DataFrame(rows)
    volume_data.columns = ['LB_ID','WP_ID','IS_PASS','WP_CMP_DATE']#Set up the column name based on a previous table
    return volume_data