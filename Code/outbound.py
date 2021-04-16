# -*- coding: utf-8 -*-
"""
Created on Thu Mar 25 08:13:59 2021

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
import numpy as np

cx_Oracle.init_oracle_client(lib_dir=r"C:\JZhou\oracle sql\instantclient_19_10")#Change the direction to where you put the oracle instant client
#For PANGUS PRD account, ask Xiaolong MA for credential.
dsnStr = cx_Oracle.makedsn("10.20.193.53", "1521", "IMSDBPROD")#The structure is (ip,port,SID)
db = cx_Oracle.connect('xiaoma','XXXXXXXXXXXXXXX123MKE',dsn=dsnStr) #The structure is (Username,Password,dsnstr)


cur = db.cursor() 
start_date = "'2021/03/15'"
end_date = "'2021/03/20'"

#For the next function, the SQL command is from Cora's script and the start_date
#and end_date will be valued while using the function.
def OUTBOUND_COLLECTION(start_date,end_date):
    command = f"SELECT LB_ID, RP_RS, TO_CHAR(SYS_CRT_DATE, 'MM/DD/YYYY HH24:MI:SS') SYS_CRT_DATE \
                    FROM IMS.TB_PM_RP_HD \
                WHERE SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'YYYY/MM/DD') AND TO_DATE({end_date}, 'YYYY/MM/DD')"
    cur.execute(command) # Execute SQL command
    rows = cur.fetchall()
    data = pd.DataFrame(rows)
    data.columns = ['LB_ID','RP_RS','SYS_CRT_DATE']#Set up the column name based on a previous table
    return data



#For the next function, the function is from Cora's script which aims to calculate
#daily outbound and inbound number.
def inbound_outbound(inbound_path, outbound_data, savepath):

    inbound = pd.read_excel(inbound_path)

    inbound = inbound[inbound['Label code'].str.startswith('W')]
    outbound = outbound_data[outbound_data['LB_ID'].str.startswith('W')]
    outbound = outbound[outbound['RP_RS'].isin([1,3])]

    inbound['QC time'] = pd.to_datetime(inbound['QC time'])
    inbound['Date'] = inbound['QC time'].dt.date
    inbound['hour'] = inbound['QC time'].dt.hour
    inbound['Shift'] = np.where(inbound['hour'].isin([0,1,2,3,4,5,6]), '3rd Shift', \
                             np.where(inbound['hour'].isin([7,8,9,10,11,12,13,14]),'1st Shift', '2nd Shift'))

    outbound['SYS_CRT_DATE'] = pd.to_datetime(outbound['SYS_CRT_DATE'])
    outbound['Date'] = outbound['SYS_CRT_DATE'].dt.date
    outbound['hour'] = outbound['SYS_CRT_DATE'].dt.hour
    outbound['Shift'] = np.where(outbound['hour'].isin([0,1,2,3,4,5,6]), '3rd Shift', \
                             np.where(outbound['hour'].isin([7,8,9,10,11,12,13,14]),'1st Shift', '2nd Shift'))

    inbound = inbound[['Date', 'Shift', 'Label code']]
    outbound = outbound[['Date', 'Shift', 'LB_ID']]

    inbound_grouped1 = pd.DataFrame(inbound.groupby(['Date', 'Shift'])['Label code'].nunique()).reset_index()
    inbound_grouped2 = pd.DataFrame(inbound_grouped1.groupby(['Date'])['Label code'].sum()).reset_index()
    inbound_grouped2['Shift'] = 'All'
    inbound_grouped = pd.concat([inbound_grouped1, inbound_grouped2])
    inbound_grouped = inbound_grouped.rename(columns={'Label code': 'Count'})
    inbound_grouped['Type'] = 'Inbound'

    outbound_grouped1 = pd.DataFrame(outbound.groupby(['Date', 'Shift'])['LB_ID'].nunique()).reset_index()
    outbound_grouped2 = pd.DataFrame(outbound_grouped1.groupby(['Date'])['LB_ID'].sum()).reset_index()
    outbound_grouped2['Shift'] = 'All'
    outbound_grouped = pd.concat([outbound_grouped1, outbound_grouped2])
    outbound_grouped = outbound_grouped.rename(columns={'LB_ID': 'Count'})
    outbound_grouped['Type'] = 'Outbound'

    summary = pd.concat([inbound_grouped, outbound_grouped])
    summary['Type'] = pd.Categorical(
            summary['Type'],
            categories=['Inbound', 'Outbound'],
            ordered=True
        )

    summary['Shift'] = pd.Categorical(
            summary['Shift'],
            categories=['1st Shift', '2nd Shift', '3rd Shift', 'All'],
            ordered=True
        )

    summary = summary.sort_values(by=['Date', 'Shift', 'Type'])
    summary = summary.pivot(index=['Date', 'Shift'], columns='Type', values='Count')
    summary['Inbound'] = summary['Inbound'].fillna(0)
    summary['Outbound'] = summary['Outbound'].fillna(0)
    summary.to_csv(savepath)
    return summary
