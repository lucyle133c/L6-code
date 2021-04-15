# -*- coding: utf-8 -*-
"""
Created on Thu Apr  1 10:06:02 2021

@author: Xiaolong.Ma
"""

import matplotlib.pyplot as plt
import datetime as dt
import pandas as pd
import numpy as np
import os
import urllib 
import urllib.request
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', None)

path = "C:/JZhou/L6" 
#Step 0: For other users, please change the path to where you want to put files
#at the first time you run this script. No need to change it in the future.
os.chdir(path)

# Download 'bsi-export_run-per-row.csv' file under 'Data Summary' folder.
temp_path = path+'/Data Summary'
url = 'http://10.20.199.186:8084/cb/bsi/bsi-export_run-per-row.csv'  
urllib.request.urlretrieve(url, os.path.join(temp_path,'bsi-export_run-per-row.csv'))

#os.chdir(r"C:\Users\ma.xiaolong\Documents\Projects\L6 Data Science\L6 Data\Essential Data\AWS Presentation\Weekly Data BSI & FT")

#wk=11
#path = f"./WK{wk} BSI Report"
#os.makedirs( path,exist_ok=True )

bsi_workpath = temp_path+r'/bsi-export_run-per-row.csv'

start_date = pd.to_datetime('2021/3/15')
end_date = pd.to_datetime('2021/4/02')

fpy_savepath = temp_path+r'C/BSI For FPY Calculation Usage only.csv'


def BSI(bsi_workpath, fpy_savepath, start_date, end_date):

    data = pd.read_csv(bsi_workpath)

    # Filter on golden board, date and product type
    data = data[data['Golden Board?'] != True]
    data['BSI Run Time'] = pd.to_datetime(data['BSI Run Time'])
    data = data[(data['BSI Run Time'].dt.date >= start_date) & (data['BSI Run Time'].dt.date <= end_date)]
    data = data[data['Board Serial Number'].str.startswith('W')]

    # Producing fpy file
    first = pd.DataFrame(data.groupby(['Board Serial Number'])['BSI Run Time'].min()).reset_index()
    first = first.merge(data, on = ['Board Serial Number', 'BSI Run Time'])
    first = first[['Board Serial Number', 'BSI Station ID', 'BSI Run Result', 'BSI Run Week Number', \
               'BSI Run Shift Number', 'BSI Run Deviation Category', 'BSI Run Time']]
    first['Date'] = first['BSI Run Time'].dt.date
    first = first.rename(columns={'BSI Run Time': 'First BSI Testing Time'})
    first.to_csv(fpy_savepath, index = False)
    
    return first 

BSI(bsi_workpath, fpy_savepath, start_date, end_date)
################################ plot the line chart ##############################
# Make sure one S/N only occurs one time. If the same S/N has both pass/fail data,
# leave the pass one. If the same S/N have several fail records, leave the earliest
# one. The case that one S/N has several pass records never shows up.

BSI_init = pd.read_csv(fpy_savepath)
BSI_board = BSI_init['Board Serial Number'].value_counts()
BSI_board = BSI_board.reset_index()
BSI_board.columns = ["Board Serial Number","Occur number"]

if BSI_board.shape[0] == BSI_init.shape[0]:
    data = BSI_init
else:
    data = pd.DataFrame()
    for i in range(BSI_board.shape[0]):
        BSI_process = BSI_init[BSI_init['Board Serial Number'] == BSI_board['Board Serial Number'][i]]
        if BSI_process.shape[0] > 1:
            BSI_process1 = BSI_process[BSI_process['BSI Run Result'] == 'PASS']
            if BSI_process1.shape[0] == 0:
                BSI_process1 = BSI_process.sort_values(by = ['Board Serial Number','First BSI Testing Time'],ascending = (True,True))
                BSI_process1 = BSI_process1.iloc[0,]
                BSI_process1 = pd.DataFrame(BSI_process1).T
                data = pd.concat([data,BSI_process1])
            elif BSI_process1.shape[0] == 1:
                data = pd.concat([data,BSI_process1])
            else:
                BSI_uni = BSI_process1.sort_values(by = ['Board Serial Number','First BSI Testing Time'],ascending = (True,True))
                BSI_uni = BSI_uni.iloc[0,]
                BSI_uni = pd.DataFrame(BSI_uni).T
                data = pd.concat([data,BSI_uni])
        else:
            data = pd.concat([data,BSI_process])
data = data.reset_index(drop = True)

# remove the Saturday and Sunday
data['Weekday'] = ''
for i in range(data.shape[0]):
    data['First BSI Testing Time'][i] = dt.datetime.strptime(data['First BSI Testing Time'][i], '%Y-%m-%d %H:%M:%S')
    data['Weekday'][i] = data['First BSI Testing Time'][i].weekday()

data = data[data['Weekday'].isin([0,1,2,3,4])]
data.drop(columns=['Weekday'],inplace=True)

# groupby by date 
FPY_data = data.groupby(['Date']).size().reset_index()
FPY_data.columns=['Date','Daily Volume']

# groupby by date and result
BSI_pass = data.groupby(['Date','BSI Run Result']).size().reset_index()
BSI_pass = BSI_pass[BSI_pass['BSI Run Result'].isin(['PASS'])]
BSI_pass.columns=['Date','BSI Run Result','PASS']

# inner join
FPY_data = FPY_data.merge(BSI_pass)
FPY_data = FPY_data.assign(FPY =  FPY_data['PASS']/FPY_data['Daily Volume'])

# Change time data type
FPY_data['Week'] = ''
for i in range(FPY_data.shape[0]):
    FPY_data['Week'][i] = dt.datetime.strptime(FPY_data['Date'][i],'%Y-%m-%d').isocalendar()[1]
FPY_data_week = FPY_data.groupby(['Week'])[['Daily Volume',"PASS"]].sum()

# x,y preperation
x = [dt.datetime.strptime(date,'%Y-%m-%d').strftime('%m-%d') for date in FPY_data['Date'] ]
y = FPY_data['FPY'].tolist()
x1 = FPY_data['Date']
x2 = [dt.datetime.strptime(a,'%Y-%m-%d').weekday() for a in x1]
y_ave = FPY_data_week["PASS"]/FPY_data_week['Daily Volume']

################################ Linear Regression ##############################
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
# Data preperation
X = list(range(len(x)))

seed = 1
validation = 0.1   
 
X_train, X_test,y_train, y_test = train_test_split(X,y,random_state=1,test_size=validation)

X_train= np.array(X_train).reshape(-1, 1)
y_train= np.array(y_train).reshape(-1, 1)
X_test = np.array(X_test).reshape(-1, 1)

#Fit model
linreg = LinearRegression()
model = linreg.fit(X_train, y_train)
b = linreg.intercept_[0]
a = linreg.coef_[0]

Y = a*pd.DataFrame(X) + b
Y = Y[0]

# line chart

fig, ax = plt.subplots(1, figsize=(16, 8))

if a > -0.1:
    cof_color = "turquoise"
else:
    cof_color = "magenta"
ax.plot(x, Y, color = cof_color, ls = '--') #linear regression line
ax.plot(x,y,color="blue")#original data points

# adjust the y-axis 
axis1 = plt.gca()
axis1.set_ylim([0,1.01])
    
# Draw weekly devided line
x_list = []
x_list_pos = []
for i in range(len(x2)):
    if x2[i] == 0:
        ax.vlines(X[i], 0,0.82, colors = "tomato", ls = '--',linewidth=1)
        x_list.append(x1[i])
        x_list_pos.append(X[i])
x_list_pos.append(X[-1])


# Mark week number and weekly fpy     
for i in range(len(x_list)):
    wk = 'WK'+str(dt.datetime.strptime(x_list[i],'%Y-%m-%d').isocalendar()[1])
    ax.annotate(wk, xy=(1/2*(x_list_pos[i]+x_list_pos[i+1])-0.5, min(y)/2),
            horizontalalignment='left',
            verticalalignment='bottom',
            fontsize=18, 
            fontweight='bold')
    ax.annotate('%.2f%%' % (y_ave[dt.datetime.strptime(x_list[i],'%Y-%m-%d').isocalendar()[1]]* 100), xy=(1/2*(x_list_pos[i]+x_list_pos[i+1])-0.5, min(y)/2-0.05),
            horizontalalignment='left',
            verticalalignment='bottom',
            fontsize=18, 
            fontweight='bold' )
# first set up the y_ticks and then set up the ticklabels  
axis1.set_yticks(np.arange(0,1.01,0.1)) # the leBSI boundary is exclusive
    
#set labels
axis1.set_yticklabels(['0','10%','20%','30%','40%','50%','60%','70%','80%','90%','100%'])
axis1.set_xticklabels(x, rotation='vertical')

plt.show()






