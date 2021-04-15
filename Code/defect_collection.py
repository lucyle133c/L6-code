# -*- coding: utf-8 -*-
"""
Created on Mon Mar 29 08:34:04 2021

@author: Jiaming.Zhou
Reminder:
To read the data in Oracle SQL, cx_Oracle package is required. To successfully 
run the package, please read the instruction in './Code/read me_cx_Oracle installation.txt'

Introduction:
This python script aims to download the defect data from each station.
"""

import cx_Oracle
import pandas as pd
import datetime

cx_Oracle.init_oracle_client(lib_dir=r"C:\JZhou\oracle sql\instantclient_19_10")#Change the direction to where you put the oracle instant client
#For PANGUS PRD account, ask Xiaolong MA for credential.
dsnStr = cx_Oracle.makedsn("10.20.193.53", "1521", "IMSDBPROD")#The structure is (ip,port,SID)
db = cx_Oracle.connect('xiaoma','foxconn123MKE',dsn=dsnStr)#The structure is (Username,Password,dsnstr)

cur = db.cursor() 

#For the following functions, the SQL command is from Cora's script and the start_date
#and end_date will be valued while using the function.
def BOT_AOI_Defect(start_date,end_date,week):
    command = f"SELECT DISTINCT WO_code, PROD_ID AS Product, LB_ID AS Label_code, WP_ID AS Station,  \
                                BAD_ITEM_NAME AS Defect_name, BAD_POINT AS Location, ' ' AS Repair, CONCAT('Rampup ', {week}) AS Week\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss') Process_completion_time, BAD_ITEM_NAME, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                   FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                   JOIN IMS.TB_BS_BAD_ITEM b ON b.BAD_ITEM_ID = t2.BAD_ITEM_ID\
                 WHERE WP_ID = 'Process009'\
                      AND LB_ID LIKE 'W%'\
                      AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND b.BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity','UpsideDown')\
                      AND t2.FIELD_EX1 != 'PASS'\
                      AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                    FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                   FROM (\
                                                                   SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                  FROM IMS.TB_PM_MO_LBWP\
                                                                               )\
                                                                 WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                      AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                      AND WP_ID ='Process009')\
                                                    WHERE IS_PASS = 'N')\
                )g\
                 WHERE rank_time = 1"
    cur.execute(command) # Execute SQL command
    rows = cur.fetchall()
    BOT_AOI_PASS_DATA = pd.DataFrame(rows)
    if BOT_AOI_PASS_DATA.shape[0] > 0:
        BOT_AOI_PASS_DATA.columns = ['WO code','Product','Label code','Station','Defect name','Location','Repair','Week']#Set up the column name based on a previous table
    return BOT_AOI_PASS_DATA


def TOP_AOI_Defect(start_date,end_date,week):
    command = f"SELECT DISTINCT WO_code, PROD_ID AS Product, LB_ID AS Label_code, WP_ID AS Station,  \
                                 BAD_ITEM_NAME AS Defect_name, BAD_POINT AS Location, ' ' AS Repair, CONCAT('Rampup ', {week}) AS Week\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss') Process_completion_time, BAD_ITEM_NAME, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                   FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                   JOIN IMS.TB_BS_BAD_ITEM b ON b.BAD_ITEM_ID = t2.BAD_ITEM_ID\
                 WHERE WP_ID = 'Process019'\
                      AND LB_ID LIKE 'W%'\
                      AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND b.BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity','UpsideDown')\
                      AND t2.FIELD_EX1 != 'PASS'\
                      AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                    FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                   FROM (\
                                                                   SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                  FROM IMS.TB_PM_MO_LBWP\
                                                                               )\
                                                                 WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                      AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                      AND WP_ID ='Process019')\
                                                    WHERE IS_PASS = 'N')\
                )g\
                 WHERE rank_time = 1"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    TOP_AOI_PASS_DATA = pd.DataFrame(rows)
    if TOP_AOI_PASS_DATA.shape[0] > 0:
        TOP_AOI_PASS_DATA.columns = ['WO code','Product','Label code','Station','Defect name','Location','Repair','Week']
    return TOP_AOI_PASS_DATA


def VI_Defect(start_date,end_date,week):
    command = f"SELECT DISTINCT WO_code, PROD_ID AS Product, LB_ID AS Label_code, WP_ID AS Station,  \
                                 BAD_ITEM_NAME AS Defect_name, BAD_POINT AS Location, ' ' AS Repair, CONCAT('Rampup ',{week}) AS Week\
                                    FROM\
                                (\
                                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss') Process_completion_time, BAD_ITEM_NAME, BAD_POINT, t2.FIELD_EX1,\
                                             RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                                   FROM IMS.TB_PM_QC_HD t1\
                                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                                   JOIN IMS.TB_BS_BAD_ITEM b ON b.BAD_ITEM_ID = t2.BAD_ITEM_ID\
                                 WHERE WP_ID = 'PTH-VI'\
                                      AND LB_ID LIKE 'W%'\
                                      AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                      AND b.BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                                      AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                                    FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                                   FROM (\
                                                                                   SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                                  FROM IMS.TB_PM_MO_LBWP\
                                                                                               )\
                                                                                 WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                                      AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                                      AND WP_ID ='PTH-VI')\
                                                                    WHERE IS_PASS = 'N')\
                                )g\
                                 WHERE rank_time = 1"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    VI_PASS_DATA = pd.DataFrame(rows)
    if VI_PASS_DATA.shape[0] > 0:
        VI_PASS_DATA.columns = ['WO code','Product','Label code','Station','Defect name','Location','Repair','Week']
    return VI_PASS_DATA

def ICT_Defect(start_date,end_date,week):
    command = f"SELECT DISTINCT WO_code, PROD_ID AS Product, LB_ID AS Label_code, WP_ID AS Station,  \
                                 BAD_ITEM_NAME AS Defect_name, BAD_POINT AS Location, ' ' AS Repair, CONCAT('Rampup ', {week}) AS Week\
                        FROM\
                    (\
                    SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss') Process_completion_time, BAD_ITEM_NAME, BAD_POINT, t2.FIELD_EX1,\
                                 RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                       FROM IMS.TB_PM_QC_HD t1\
                       JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                       JOIN IMS.TB_BS_BAD_ITEM b ON b.BAD_ITEM_ID = t2.BAD_ITEM_ID\
                     WHERE WP_ID = 'PTH-ICT'\
                          AND LB_ID LIKE 'W%'\
                          AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                          AND b.BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                          AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                        FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                       FROM (\
                                                                       SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                      FROM IMS.TB_PM_MO_LBWP\
                                                                                   )\
                                                                     WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                          AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                          AND WP_ID ='PTH-ICT')\
                                                        WHERE IS_PASS = 'N')\
                    )g\
                     WHERE rank_time = 1"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    ICT_PASS_DATA = pd.DataFrame(rows)
    if ICT_PASS_DATA.shape[0] > 0:
        ICT_PASS_DATA.columns = ['WO code','Product','Label code','Station','Defect name','Location','Repair','Week']
    return ICT_PASS_DATA


def OST_Defect(start_date,end_date,week):
    command = f"SELECT DISTINCT WO_code, PROD_ID AS Product, LB_ID AS Label_code, WP_ID AS Station,  \
                                 BAD_ITEM_NAME AS Defect_name, BAD_POINT AS Location, ' ' AS Repair, CONCAT('Rampup ', {week}) AS Week\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss') Process_completion_time, BAD_ITEM_NAME, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                   FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                   JOIN IMS.TB_BS_BAD_ITEM b ON b.BAD_ITEM_ID = t2.BAD_ITEM_ID\
                 WHERE WP_ID = 'PTH-OST'\
                      AND LB_ID LIKE 'W%'\
                      AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND b.BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                      AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                    FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                   FROM (\
                                                                   SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                  FROM IMS.TB_PM_MO_LBWP\
                                                                               )\
                                                                 WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                      AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                      AND WP_ID ='PTH-OST')\
                                                    WHERE IS_PASS = 'N')\
                )g\
                 WHERE rank_time = 1"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    OST_PASS_DATA = pd.DataFrame(rows)
    if OST_PASS_DATA.shape[0] > 0:
        OST_PASS_DATA.columns = ['WO code','Product','Label code','Station','Defect name','Location','Repair','Week']
    return OST_PASS_DATA


def PACK_AOI_Defect(start_date,end_date,week):
    command = f"SELECT DISTINCT WO_code, PROD_ID AS Product, LB_ID AS Label_code, WP_ID AS Station,  \
                                 BAD_ITEM_NAME AS Defect_name, BAD_POINT AS Location, ' ' AS Repair, CONCAT('Rampup ', {week}) AS Week\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss') Process_completion_time, BAD_ITEM_NAME, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                   FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                   JOIN IMS.TB_BS_BAD_ITEM b ON b.BAD_ITEM_ID = t2.BAD_ITEM_ID\
                 WHERE WP_ID = 'PTH-Packing-AOI'\
                      AND LB_ID LIKE 'W%'\
                      AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND b.BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                      AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                    FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                   FROM (\
                                                                   SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                  FROM IMS.TB_PM_MO_LBWP\
                                                                               )\
                                                                 WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                      AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                      AND WP_ID ='PTH-Packing-AOI')\
                                                    WHERE IS_PASS = 'N')\
                )g\
                 WHERE rank_time = 1"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    PACK_AOI_PASS_DATA = pd.DataFrame(rows)
    if PACK_AOI_PASS_DATA.shape[0] > 0:
        PACK_AOI_PASS_DATA.columns = ['WO code','Product','Label code','Station','Defect name','Location','Repair','Week']
    return PACK_AOI_PASS_DATA

