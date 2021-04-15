# -*- coding: utf-8 -*-
"""
Created on Thu Apr  1 09:41:26 2021

@author: Jiaming.Zhou

  Program Summary
  Purpose : Compare the produce efficiency between different shifts
  Method: Kruskal-Wallis rank sum test(between multi-group, nonparametric test)
          Wilcox rank sum test(between two groups, nonparametric test)
  Inputs : Volume summary file
  Outputs : Test result
  Side Effects :
"""
import os
import pandas as pd
import numpy as np
import datetime
from scipy import stats
from scipy.stats import ranksums
path = r"C:\JZhou\L6\Data Summary\volume summary" 
os.chdir(path)
data = pd.read_csv(r'volume_from_2021-1-04_till_2021-3-31.csv')
station_list = ["BOT", "TOP", "VI", "OST", "ICT", "PACK","BSI","FT1","FT2"]

#Devide the data based on station and shift(Total)
for station in station_list:
    data1 = data[data['Shift']=="1st Shift"]
    data1 = data1[data1['Result'] == "Total"][station]
    data2 = data[data['Shift']=="2nd Shift"]
    data2 = data2[data2['Result'] == "Total"][station]
    data3 = data[data['Shift']=="3rd Shift"]
    data3 = data3[data3['Result'] == "Total"][station]

    print(station)
    print(stats.kruskal(data1,data2,data3))# Kruskal-Wallis rank sum test
    print(ranksums(data1, data2))#Wilcox rank sum test
    print(np.median(data1))
    print(np.median(data2))
    print(np.mean(data1))
    print(np.mean(data2))
    print(np.median(data3))
#Devide the data based on station and shift(FPY)
for station in station_list:
    data1 = data[data['Shift']=="1st Shift"]
    data1 = data1[data1['Result'] == "Fpy"][station]
    data2 = data[data['Shift']=="2nd Shift"]
    data2 = data2[data2['Result'] == "Fpy"][station]
    data3 = data[data['Shift']=="3rd Shift"]
    data3 = data3[data3['Result'] == "Fpy"][station]

    print(station)
    print(stats.kruskal(data1,data2,data3))
#    print(ranksums(data1, data2))
#    print(np.median(data1))
#    print(np.median(data2))
    print(np.mean(data1))
    print(np.mean(data2))
#    print(np.median(data3))
    print(np.mean(data3))
