# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 10:46:51 2021

@author: Jiaming.Zhou
Reminder:
To read the data in Oracle SQL, cx_Oracle package is required. To successfully 
run the package, please read the instruction in './Code/read me_cx_Oracle installation.txt'

Introduction:
This python script aims to download the pass/fail data from each station.
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
def BOT_AOI_PASS(start_date,end_date):
    command = f"SELECT LB_ID, WP_ID, IS_PASS, TO_CHAR(WP_CMP_DATE, 'MM/DD/YYYY') Process_completion_time\
                FROM\
                (\
                    SELECT  LB_ID, WP_ID, IS_PASS,\
                    WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                    FROM IMS.TB_PM_MO_LBWP WHERE WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                    )\
                WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                    AND WP_ID = 'Process009'\
                    AND IS_PASS = 'Y'\
                    AND LB_ID LIKE 'W%'"
    cur.execute(command)  # Execute SQL command
    rows = cur.fetchall()
    BOT_AOI_PASS_DATA = pd.DataFrame(rows)
    if BOT_AOI_PASS_DATA.shape[0] > 0:
        BOT_AOI_PASS_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']#Set up the column name based on a previous table
    return BOT_AOI_PASS_DATA

def BOT_AOI_FAIL(start_date,end_date):
    command = f"SELECT DISTINCT LB_ID, WP_ID, IS_PASS, Process_completion_time\
                FROM\
            (\
            SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY') Process_completion_time, BAD_ITEM_ID, BAD_POINT, t2.FIELD_EX1,\
                         RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
               FROM IMS.TB_PM_QC_HD t1\
               JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
             WHERE WP_ID = 'Process009'\
                  AND LB_ID LIKE 'W%'\
                  AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                  AND BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                  AND t2.FIELD_EX1 != 'PASS'\
                  AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                               FROM (\
                                                               SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                              FROM IMS.TB_PM_MO_LBWP \
                                                                           )\
                                                             WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                   AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                  AND WP_ID = 'Process009')\
                                                WHERE IS_PASS = 'N')\
            )g\
             WHERE rank_time = 1"
             
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    BOT_AOI_FAIL_DATA = pd.DataFrame(rows)
    if BOT_AOI_FAIL_DATA.shape[0] > 0:
        BOT_AOI_FAIL_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return BOT_AOI_FAIL_DATA


def TOP_AOI_PASS(start_date,end_date):
    command = f"SELECT LB_ID, WP_ID, IS_PASS, TO_CHAR(WP_CMP_DATE, 'MM/DD/YYYY') Process_completion_time\
                  FROM\
                (\
                SELECT  LB_ID, WP_ID, IS_PASS,\
                WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                FROM IMS.TB_PM_MO_LBWP WHERE WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                )\
                WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                     AND WP_ID = 'Process019'\
                     AND IS_PASS = 'Y'\
                     AND LB_ID LIKE 'W%'"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    TOP_AOI_PASS_DATA = pd.DataFrame(rows)
    if TOP_AOI_PASS_DATA.shape[0] > 0:
        TOP_AOI_PASS_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return TOP_AOI_PASS_DATA

def TOP_AOI_FAIL(start_date,end_date):
    command = f"SELECT DISTINCT LB_ID, WP_ID, IS_PASS, Process_completion_time\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY') Process_completion_time, BAD_ITEM_ID, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                   FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                 WHERE WP_ID = 'Process019'\
                      AND LB_ID LIKE 'W%'\
                      AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                      AND t2.FIELD_EX1 != 'PASS'\
                      AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                    FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                   FROM (\
                                                                   SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                  FROM IMS.TB_PM_MO_LBWP \
                                                                               )\
                                                                 WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                       AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                      AND WP_ID = 'Process019')\
                                                    WHERE IS_PASS = 'N')\
                )g\
                 WHERE rank_time = 1"
             
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    TOP_AOI_FAIL_DATA = pd.DataFrame(rows)
    if TOP_AOI_FAIL_DATA.shape[0] > 0:
        TOP_AOI_FAIL_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return TOP_AOI_FAIL_DATA

def VI_PASS(start_date,end_date):
    command = f"SELECT LB_ID, WP_ID, IS_PASS, TO_CHAR(WP_CMP_DATE, 'MM/DD/YYYY') Process_completion_time\
                  FROM\
                (\
                SELECT  LB_ID, WP_ID, IS_PASS,\
                WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                FROM IMS.TB_PM_MO_LBWP WHERE WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                )\
                WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                     AND WP_ID = 'PTH-VI'\
                     AND IS_PASS = 'Y'\
                     AND LB_ID LIKE 'W%'"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    VI_PASS_DATA = pd.DataFrame(rows)
    if VI_PASS_DATA.shape[0] > 0:
        VI_PASS_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return VI_PASS_DATA

def VI_FAIL(start_date,end_date):
    command = f"SELECT DISTINCT LB_ID, WP_ID, IS_PASS, Process_completion_time\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY') Process_completion_time, BAD_ITEM_ID, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                   FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                 WHERE WP_ID = 'PTH-VI'\
                      AND LB_ID LIKE 'W%'\
                      AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                      AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                    FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                   FROM (\
                                                                   SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                  FROM IMS.TB_PM_MO_LBWP \
                                                                               )\
                                                                 WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                       AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                      AND WP_ID = 'PTH-VI')\
                                                    WHERE IS_PASS = 'N')\
                )g\
                 WHERE rank_time = 1"
             
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    VI_FAIL_DATA = pd.DataFrame(rows)
    if VI_FAIL_DATA.shape[0] > 0:
        VI_FAIL_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return VI_FAIL_DATA

def ICT_PASS(start_date,end_date):
    command = f"SELECT LB_ID, WP_ID, IS_PASS, TO_CHAR(WP_CMP_DATE, 'MM/DD/YYYY') Process_completion_time\
                  FROM\
                (\
                SELECT  LB_ID, WP_ID, IS_PASS,\
                WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                FROM IMS.TB_PM_MO_LBWP WHERE WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                )\
                WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                     AND WP_ID = 'PTH-ICT'\
                     AND IS_PASS = 'Y'\
                     AND LB_ID LIKE 'W%'"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    ICT_PASS_DATA = pd.DataFrame(rows)
    if ICT_PASS_DATA.shape[0] > 0:
        ICT_PASS_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return ICT_PASS_DATA

def ICT_FAIL(start_date,end_date):
    command = f"SELECT DISTINCT LB_ID, WP_ID, IS_PASS, Process_completion_time\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY') Process_completion_time, BAD_ITEM_ID, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                   FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                 WHERE WP_ID = 'PTH-ICT'\
                      AND LB_ID LIKE 'W%'\
                      AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                      AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                    FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                   FROM (\
                                                                   SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                  FROM IMS.TB_PM_MO_LBWP \
                                                                               )\
                                                                 WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                       AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                      AND WP_ID = 'PTH-ICT')\
                                                    WHERE IS_PASS = 'N')\
                )g\
                 WHERE rank_time = 1"
             
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    ICT_FAIL_DATA = pd.DataFrame(rows)
    if ICT_FAIL_DATA.shape[0] > 0:
        ICT_FAIL_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return ICT_FAIL_DATA

def OST_PASS(start_date,end_date):
    command = f"SELECT LB_ID, WP_ID, IS_PASS, TO_CHAR(WP_CMP_DATE, 'MM/DD/YYYY') Process_completion_time\
                  FROM\
                (\
                SELECT  LB_ID, WP_ID, IS_PASS,\
                WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                FROM IMS.TB_PM_MO_LBWP WHERE WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                )\
                WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                     AND WP_ID = 'PTH-OST'\
                     AND IS_PASS = 'Y'\
                     AND LB_ID LIKE 'W%'"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    OST_PASS_DATA = pd.DataFrame(rows)
    if OST_PASS_DATA.shape[0] > 0:
        OST_PASS_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return OST_PASS_DATA

def OST_FAIL(start_date,end_date):
    command = f"SELECT DISTINCT LB_ID, WP_ID, IS_PASS, Process_completion_time\
                    FROM\
                (\
                SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY') Process_completion_time, BAD_ITEM_ID, BAD_POINT, t2.FIELD_EX1,\
                             RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                   FROM IMS.TB_PM_QC_HD t1\
                   JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                 WHERE WP_ID = 'PTH-OST'\
                      AND LB_ID LIKE 'W%'\
                      AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                      AND BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                      AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                    FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                   FROM (\
                                                                   SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                  FROM IMS.TB_PM_MO_LBWP \
                                                                               )\
                                                                 WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                       AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                      AND WP_ID = 'PTH-OST')\
                                                    WHERE IS_PASS = 'N')\
                )g\
                 WHERE rank_time = 1"
             
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    OST_FAIL_DATA = pd.DataFrame(rows)
    if OST_FAIL_DATA.shape[0] > 0:
        OST_FAIL_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return OST_FAIL_DATA

def PACK_AOI_PASS(start_date,end_date):
    command = f"SELECT LB_ID, WP_ID, IS_PASS, TO_CHAR(WP_CMP_DATE, 'MM/DD/YYYY') Process_completion_time\
                  FROM\
                (\
                SELECT  LB_ID, WP_ID, IS_PASS,\
                WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                FROM IMS.TB_PM_MO_LBWP WHERE WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                )\
                WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                     AND WP_ID = 'PTH-Packing-AOI'\
                     AND IS_PASS = 'Y'\
                     AND LB_ID LIKE 'W%'"
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    PACK_AOI_PASS_DATA = pd.DataFrame(rows)
    if PACK_AOI_PASS_DATA.shape[0] > 0:
        PACK_AOI_PASS_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return PACK_AOI_PASS_DATA

def PACK_AOI_FAIL(start_date,end_date):
    command = f"SELECT DISTINCT LB_ID, WP_ID, IS_PASS, Process_completion_time\
                        FROM\
                    (\
                    SELECT MO as WO_code, LB_ID, PROD_ID, WP_ID, IS_PASS, TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY') Process_completion_time, BAD_ITEM_ID, BAD_POINT, t2.FIELD_EX1,\
                                 RANK()OVER(PARTITION BY LB_ID ORDER BY TO_DATE(TO_CHAR(t1.SYS_CRT_DATE, 'MM/DD/YYYY hh24:mi:ss'), 'MM/DD/YYYY hh24:mi:ss')) rank_time\
                       FROM IMS.TB_PM_QC_HD t1\
                       JOIN IMS.TB_PM_QC_DT t2 ON t1.QC_ID = t2.QC_ID AND t1.SYS_CRT_DATE = t2.SYS_CRT_DATE\
                     WHERE WP_ID = 'PTH-Packing-AOI'\
                          AND LB_ID LIKE 'W%'\
                          AND t1.SYS_CRT_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                          AND BAD_ITEM_ID NOT IN ('Dimension', 'OCROCV', 'Coplanarity')\
                          AND LB_ID IN (SELECT DISTINCT LB_ID \
                                                        FROM (SELECT LB_ID,  WP_ID, IS_PASS, WP_CMP_DATE\
                                                                       FROM (\
                                                                       SELECT  LB_ID, WP_ID, IS_PASS, WP_CMP_DATE,  MIN (WP_CMP_DATE) OVER (PARTITION BY LB_ID, WP_ID) PROCESS_COMPLETION\
                                                                                      FROM IMS.TB_PM_MO_LBWP \
                                                                                   )\
                                                                     WHERE WP_CMP_DATE = PROCESS_COMPLETION\
                                                                           AND WP_CMP_DATE BETWEEN TO_DATE({start_date}, 'MM/DD/YYYY') AND TO_DATE({end_date}, 'MM/DD/YYYY')\
                                                                          AND WP_ID = 'PTH-Packing-AOI')\
                                                        WHERE IS_PASS = 'N')\
                    )g\
                     WHERE rank_time = 1"
             
    cur.execute(command) # 执行sql语句
    rows = cur.fetchall()
    PACK_AOI_FAIL_DATA = pd.DataFrame(rows)
    if PACK_AOI_FAIL_DATA.shape[0] > 0:
        PACK_AOI_FAIL_DATA.columns = ['LB_ID','WP_ID','IS_PASS','PROCESS_COMPLETION_TIME']
    return PACK_AOI_FAIL_DATA