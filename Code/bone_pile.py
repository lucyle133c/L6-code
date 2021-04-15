# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 14:52:27 2021

@author: Jiaming.Zhou
"""

import cx_Oracle 
import pandas as pd
import datetime
import numpy as np
cx_Oracle.init_oracle_client(lib_dir=r"C:\JZhou\oracle sql\instantclient_19_10")#Change the direction to where you put the oracle instant client
#For PANGUS PRD account, ask Xiaolong MA for credential.
dsnStr = cx_Oracle.makedsn("10.20.193.53", "1521", "IMSDBPROD")#The structure is (ip,port,SID)
db = cx_Oracle.connect('xiaoma','foxconn123MKE',dsn=dsnStr)#The structure is (Username,Password,dsnstr)

cur = db.cursor() 


#For the next function, the SQL command is from Cora's script and the start_date
#and end_date will be valued while using the function.
def bone_pile1(start_date,end_date):
    command = f"SELECT DISTINCT WO_code, PROD_ID AS Product, LB_ID AS Label_code, WP_ID AS Station,  \
                                                 BAD_ITEM_NAME AS Defect_name, BAD_POINT AS Location, Process_completion_time\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss') Process_completion_time, BAD_ITEM_NAME, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID, WP_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss') DESC) rank_time\
                  FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                   JOIN IMS.TB_BS_BAD_ITEM b ON b.BAD_ITEM_ID = t2.BAD_ITEM_ID\
                 WHERE WP_ID IN ('PTH-Packing-AOI',  'PTH-OST')\
                      AND t1.SYS_CRT_DATE BETWEEN  TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND b.BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                      AND LB_ID LIKE 'W%'\
                )g\
                 WHERE rank_time = 1"
    cur.execute(command) # Execute SQL command
    rows = cur.fetchall()
    data1 = pd.DataFrame(rows)
    data1.columns = ['WO_CODE','PRODUCT','LABEL_CODE','STATION','DEFECT_NAME','LOCATION','PROCESS_COMPLETION_TIME']#Set up the column name based on a previous table
    return data1


def bone_pile2(start_date,end_date):
    command = f"SELECT DISTINCT WO_code, PROD_ID AS Product, LB_ID AS Label_code, WP_ID AS Station,  \
                                 BAD_ITEM_NAME AS Defect_name, BAD_POINT AS Location, Process_completion_time\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss') Process_completion_time, BAD_ITEM_NAME, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID, WP_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss') DESC) rank_time\
                  FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                   JOIN IMS.TB_BS_BAD_ITEM b ON b.BAD_ITEM_ID = t2.BAD_ITEM_ID\
                 WHERE WP_ID IN ('Process009',  'Process019')\
                      AND t2.FIELD_EX1 != 'PASS'\
                      AND t1.SYS_CRT_DATE BETWEEN  TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND b.BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                      AND LB_ID LIKE 'W%'\
                          )g\
                 WHERE rank_time = 1"
    cur.execute(command) # Execute SQL command
    rows = cur.fetchall()
    data2 = pd.DataFrame(rows)
    data2.columns = ['WO_CODE','PRODUCT','LABEL_CODE','STATION','DEFECT_NAME','LOCATION','PROCESS_COMPLETION_TIME']#Set up the column name based on a previous table
    return data2


def bone_repair(repair_path, bsi_path, ft_path, ict_path, defect_path, savepath1, savepath2):

    data = pd.read_excel(repair_path)
    bsi = pd.read_csv(bsi_path)
    ft = pd.read_csv(ft_path)
    ict = pd.read_csv(ict_path)
    defects = defect_path

    # create a df for defects in stations other than testing
    df1 = data.merge(defects, left_on = ['Label code','Check process code'], right_on = ['LABEL_CODE', 'STATION'])
    df1 = df1[['Label code','Check process code', 'DEFECT_NAME', 'LOCATION', 'QC time']]
    df1 = df1.rename(columns = {'DEFECT_NAME' : 'Defect_name',
                                    'LOCATION': 'Location'})
    df1 = df1.drop_duplicates()

    # create a df for defects for testing
    ft['Station'] = np.where(ft['FT Station ID'].str.startswith('FT1'), 'PTH-FT1', 'PTH-FT2')
    bsi = bsi.rename(columns = {'BSI Run Time': 'Run Time',
                                'BSI Run Result': 'Run Result',
                                'BSI Run Summary': 'Run Summary',
                                'BSI Run Deviation Category': 'Run Deviation Category'})
    bsi['Station'] = 'PTH-BSI'
    ict = ict.rename(columns = {'ICT Run Time': 'Run Time',
                                'ICT Run Result': 'Run Result',
                                'ICT Run Summary': 'Run Summary',
                                'ICT Run Deviation Category': 'Run Deviation Category'})
    ict['Station'] = 'PTH-ICT'
    ft = ft.rename(columns = {'FT Run Time': 'Run Time',
                                'FT Run Result': 'Run Result',
                                'FT Run Summary': 'Run Summary',
                                'FT Run Deviation Category': 'Run Deviation Category'})

    bsi = bsi[['Board Serial Number', 'Run Time', 'Run Result', 'Run Summary', 'Run Deviation Category', 'Station']]
    ict = ict[['Board Serial Number', 'Run Time', 'Run Result', 'Run Summary', 'Run Deviation Category', 'Station']]
    ft = ft[['Board Serial Number', 'Run Time', 'Run Result', 'Run Summary', 'Run Deviation Category', 'Station']]

    def testing(test):

        test['Run Time'] = pd.to_datetime(test['Run Time'])
        test_grouped = test.groupby('Board Serial Number')['Run Time'].max()
        test_grouped = pd.DataFrame(test_grouped).reset_index()
        test_grouped = test_grouped.merge(test, on = ['Board Serial Number', 'Run Time'])
        test_grouped = test_grouped[['Board Serial Number', 'Run Time', 'Run Deviation Category', 'Run Summary', 'Station']]
        test_grouped = test_grouped[test_grouped['Run Deviation Category'] != 'PASS']
        test_grouped = test_grouped.drop_duplicates()

        return test_grouped

    bsi_grouped = testing(bsi)
    ict_grouped = testing(ict)
    ft_grouped = testing(ft)

    testing = pd.concat([bsi_grouped, ict_grouped, ft_grouped])
    df2 = data.merge(testing, left_on = ['Label code','Check process code'], right_on = ['Board Serial Number', 'Station'])
    df2 = df2[['Label code','Check process code', 'Run Deviation Category', 'Run Summary', 'QC time']]
    df2 = df2.rename(columns = {'Run Deviation Category' : 'Defect_name',
                                'Run Summary': 'Location'})
    df2 = df2.drop_duplicates()

    historical_torepair = pd.concat([df2, df1])
    historical_torepair['QC time'] = pd.to_datetime(historical_torepair['QC time'])
    historical_torepair['Duration'] = (pd.datetime.today() - historical_torepair['QC time']).dt.days

    # Remove boards that have occurred > 50
    historical_grouped = pd.DataFrame(historical_torepair.groupby('Label code').size().reset_index())
    historical_grouped = historical_grouped.rename(columns = {0 : 'Count'})
    historical_grouped = historical_grouped[historical_grouped['Count'] < 50]
    historical_labels = historical_grouped['Label code']
    historical_torepair = historical_torepair[historical_torepair['Label code'].isin(historical_labels)]

    historical_without_location = historical_torepair[['Label code', 'Check process code', 'Defect_name', 'QC time', 'Duration']]
    historical_without_location = historical_without_location.drop_duplicates()

    historical_without_location.to_csv(savepath1,index = False)
    historical_torepair.to_csv(savepath2,index = False)
     