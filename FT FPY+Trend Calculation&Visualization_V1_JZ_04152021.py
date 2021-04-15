# -*- coding: utf-8 -*-
"""
Created on Thu Apr  1 10:06:00 2021

@author: Xiaolong.Ma/Jiaming Zhou

  Program Summary
  Purpose : Calculte three weeks' FT FPY, Analysis the trend and Draw the plot
  Inputs : ft-export_run-per-row.csv
  Outputs : Three weeks' daily FT FPY data, linear regression line and week average fpy
  Side Effects :
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
#path = f"./WK{wk} FT Report"
#os.makedirs( path,exist_ok=True )

FT_workpath = temp_path+r'/bsi-export_run-per-row.csv'

start_date = pd.to_datetime('2021/3/15')
end_date = pd.to_datetime('2021/4/02')

fpy_savepath = temp_path+r'C/BSI For FPY Calculation Usage only.csv'


def FT(FT_workpath, fpy_savepath, start_date, end_date):

    data = pd.read_csv(FT_workpath)

    # Filter on golden board, date and product type
    data = data[data['Golden Board?'] != True]
    data['FT Run Time'] = pd.to_datetime(data['FT Run Time'])
    data = data[(data['FT Run Time'].dt.date >= start_date) & (data['FT Run Time'].dt.date <= end_date)]
    data = data[data['Board Serial Number'].str.startswith('W')]

    # Producing fpy file
    first = pd.DataFrame(data.groupby(['Board Serial Number'])['FT Run Time'].min()).reset_index()
    first = first.merge(data, on = ['Board Serial Number', 'FT Run Time'])
    first = first[['Board Serial Number', 'FT Station ID', 'FT Run Result', 'FT Run Week Number', \
               'FT Run Shift Number', 'FT Run Deviation Category', 'FT Run Time']]
    first['Date'] = first['FT Run Time'].dt.date
    first = first.rename(columns={'FT Run Time': 'First FT Testing Time'})
    first.to_csv(fpy_savepath, index = False)
    
    return first

FT(FT_workpath, fpy_savepath, start_date, end_date)

######### plot the line chart ################
FT_init = pd.read_csv(fpy_savepath)
FT_board = FT_init['Board Serial Number'].value_counts()
FT_board = FT_board.reset_index()
FT_board.columns = ["Board Serial Number","Occur number"]
if FT_board.shape[0] == FT_init.shape[0]:
    data = FT_init
else:
    data = pd.DataFrame()
    for i in range(FT_board.shape[0]):
        FT_process = FT_init[FT_init['Board Serial Number'] == FT_board['Board Serial Number'][i]]
        if FT_process.shape[0] > 1:
            FT_process1 = FT_process[FT_process['BSI Run Result'] == 'PASS']
            if FT_process1.shape[0] == 0:
                FT_process1 = FT_process.sort_values(by = ['Board Serial Number','First FT Testing Time'],ascending = (True,True))
                FT_process1 = FT_process1.iloc[0,]
                FT_process1 = pd.DataFrame(FT_process1).T
                data = pd.concat([data,FT_process1])
            elif FT_process1.shape[0] == 1:
                data = pd.concat([data,FT_process1])
            else:
                FT_uni = FT_process1.sort_values(by = ['Board Serial Number','First FT Testing Time'],ascending = (True,True))
                FT_uni = FT_uni.iloc[0,]
                FT_uni = pd.DataFrame(FT_uni).T
                data = pd.concat([data,FT_uni])
        else:
            data = pd.concat([data,FT_process])
data = data.reset_index(drop = True)

# remove the Saturday and Sunday
data['Weekday'] = ''
for i in range(data.shape[0]):
    data['First FT Testing Time'][i] = dt.datetime.strptime(data['First FT Testing Time'][i], '%Y-%m-%d %H:%M:%S')
    data['Weekday'][i] = data['First FT Testing Time'][i].weekday()
    
data = data[data['Weekday'].isin([0,1,2,3,4])]
data.drop(columns=['Weekday'],inplace=True)

# groupby by date 
FPY_data = data.groupby(['Date']).size().reset_index()

FPY_data.columns=['Date','Daily Volume']

# groupby by date and result
FT_pass = data.groupby(['Date','FT Run Result']).size().reset_index()
FT_pass = FT_pass[FT_pass['FT Run Result'].isin(['PASS'])]
FT_pass.columns=['Date','FT Run Result','PASS']

# inner join
FPY_data = FPY_data.merge(FT_pass)
FPY_data = FPY_data.assign(FPY =  FPY_data['PASS']/FPY_data['Daily Volume'])

FPY_data['Week'] = ''
for i in range(FPY_data.shape[0]):
    FPY_data['Week'][i] = dt.datetime.strptime(FPY_data['Date'][i],'%Y-%m-%d').isocalendar()[1]
FPY_data_week = FPY_data.groupby(['Week'])[['Daily Volume',"PASS"]].sum()

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
axis1.set_yticks(np.arange(0,1.01,0.1)) # the left boundary is exclusive
    
#set labels
axis1.set_yticklabels(['0','10%','20%','30%','40%','50%','60%','70%','80%','90%','100%'])
axis1.set_xticklabels(x, rotation='vertical')

plt.show()







