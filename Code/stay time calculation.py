# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 09:48:29 2021

@author: Jiaming.Zhou
"""
import os
import pandas as pd
import numpy as np
import datetime
import time
import re
d = pd.read_excel(r"C:\Users\Jiaming.Zhou\Desktop\Repaired Boards_1.xlsx")

d_adjust = d.loc[:,['WO code','Product','Tech code','Label code','Create time','Interval time/S']]

index_list = []
for i in range(d_adjust.shape[0]):
    if d_adjust['Label code'][i][0] == 'A' or d_adjust['Label code'][i][0] == 'M':
        index_list.append(i)
        
        
data = d_adjust.iloc[index_list,:]

data1 = data.reset_index(drop = True)
data1['Repair Duration Days'] = None
data1['Repair Duration Hours and Minutes'] = None
def calculate_day(t):
    day = t//(3600*24)
    return(day)

def calculate_time(t,day = 0,hour = 0,minute = 0, second = 0):
    day = t//(3600*24)
    res_t = t-3600*24*day
    day = str(day)
    hour = res_t//3600
    res_t = res_t-3600*hour
    hour = str(hour)
    minute = res_t//60
    second = res_t-60*minute
    minute = str(minute)
    second = str(second)
    if len(hour)<2:
        hour = '0'+hour
    if len(minute)<2:
        minute = '0'+minute
    if len(second)<2:
        second = '0'+second
    return(f"{hour}:{minute}:{second}")



for i in range(data1.shape[0]):
    data1['Repair Duration Days'][i] = calculate_day(data1['Interval time/S'][i])   
    data1['Repair Duration Hours and Minutes'][i] = calculate_time(data1['Interval time/S'][i])

data1 = data1.drop(columns = 'Interval time/S')   
data1.to_csv(r"C:\Users\Jiaming.Zhou\Desktop\board stay time.csv",index = False)





