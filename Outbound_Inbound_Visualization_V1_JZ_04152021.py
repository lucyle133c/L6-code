# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 08:47:29 2021

@author: Jiaming.Zhou

Introduction:
This python script helps engineer to visualize the outbound and inbound data. 
There are three pictures that can be generated in this script. Daily total inbound
and outbound volume vertical bar chart, per shift inbound and outbound volume 
vertical bar chart, Daily inbound and outbound difference volume horizon bar chart.
"""

import os
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
path = "C:\JZhou\L6"
#Step 0: For other users, please change the path to where you want to put files
#at the first time you run this script. No need to change it in the future.
os.chdir(path)

end_date = '4-12'#Step 1: Type in the end date in another format. Make sure you have 'Repair Summary till {end_date}.csv' file.
savepath = os.path.join(path,f'Data Summary\inbound_outbound summary\Repair Summary till {end_date}.csv')
outbound_inbound_result = pd.read_csv(savepath)

#outbound_inbound_result = outbound_inbound_result[outbound_inbound_result['Date']>="2021-03-29"]#Step 5: Determine the 
#outbound_inbound_result = outbound_inbound_result[outbound_inbound_result['Date']<="2021-04-02"]
week_date = outbound_inbound_result['Date'].unique()
start_date = datetime.datetime.strptime(week_date[0], '%m/%d/%Y').strftime('%m-%d-%Y')
end_date = datetime.datetime.strptime(week_date[-1], '%m/%d/%Y').strftime('%m-%d-%Y')

for date in week_date:
    temp = outbound_inbound_result[outbound_inbound_result['Date'] == date]
    if temp.shape[0] < 4:
        if "All" not in list(temp['Shift']):
            temp_add = pd.DataFrame([date,"ALL",0,0]).T
            temp_add.columns = outbound_inbound_result.columns
            outbound_inbound_result = pd.concat([outbound_inbound_result,temp_add])
        if "1st Shift" not in list(temp['Shift']):
            temp_add = pd.DataFrame([date,"1st Shift",0,0]).T
            temp_add.columns = outbound_inbound_result.columns
            outbound_inbound_result = pd.concat([outbound_inbound_result,temp_add])
        if "2nd Shift" not in list(temp['Shift']):
            temp_add = pd.DataFrame([date,"2nd Shift",0,0]).T
            temp_add.columns = outbound_inbound_result.columns
            outbound_inbound_result = pd.concat([outbound_inbound_result,temp_add])
        if "3rd Shift" not in list(temp['Shift']):
            temp_add = pd.DataFrame([date,"3rd Shift",0,0]).T
            temp_add.columns = outbound_inbound_result.columns
            outbound_inbound_result = pd.concat([outbound_inbound_result,temp_add])
            
outbound_inbound_result = outbound_inbound_result.sort_values(by = ['Date','Shift'],ascending = (True,True))
data = outbound_inbound_result[outbound_inbound_result['Shift']=="All"]
data1 = outbound_inbound_result[outbound_inbound_result['Shift']=="1st Shift"]
data2 = outbound_inbound_result[outbound_inbound_result['Shift']=="2nd Shift"]
data3 = outbound_inbound_result[outbound_inbound_result['Shift']=="3rd Shift"]
data = data.reset_index(drop = True)
data1 = data1.reset_index(drop = True)
data2 = data2.reset_index(drop = True)
data3 = data3.reset_index(drop = True)
outbound = data['Outbound']
inbound = data['Inbound']
outbound1 = data1['Outbound']
inbound1 = data1['Inbound']
outbound2 = data2['Outbound']
inbound2 = data2['Inbound']
outbound3 = data3['Outbound']
inbound3 = data3['Inbound']
X = np.arange(len(week_date))
Y1 = inbound
Y2 = outbound
Y1_1 = inbound1
Y2_1 = outbound1
Y1_2 = inbound2
Y2_2 = outbound2
Y1_3 = inbound3
Y2_3 = outbound3
diff = inbound - outbound
X1 = []
X2 = []
diff1 = []
diff2 = []
for i in range(len(diff)):
    if diff[i]>0:
        X1.append(i)
        diff1.append(int(diff[i]))
    else:
        X2.append(i)
        diff2.append(int(diff[i]))
        
Y_max = max(max(Y1_1),max(Y2_1),max(Y1_2),max(Y2_2),max(Y1_3),max(Y2_3),max(abs(diff)))
'''
Repair Summary picture
'''
plt.figure(figsize=(20, 10))
plt.bar(X,+Y1,color = "red", label = "Inbound",width=0.5)
plt.bar(X,-Y2,color = "green", label = "Outbound",width=0.5)
for x,y in zip(X,Y1):
    plt.text(x,y+0.05,'%.0f' %y, ha='center',va='bottom')
 
for x, y in zip(X, Y2):
    plt.text(x,-y-5,'%.0f'%y, ha='center',va='bottom')
    

plt.xticks(X,week_date)

plt.title(f'Outbound & Inbound Total Data from {start_date} - {end_date}', fontdict={'size':20})
plt.ylabel("Amount",fontsize = 15)
plt.xlabel("Date",fontsize = 15)
plt.legend(loc='upper right', borderpad=2)
plt.ylim( -max(max(Y1),max(Y2))-20,max(max(Y1),max(Y2))+20)
plt.show()
plt.savefig(f'./Data Summary/inbound_outbound summary/Repair Summary from {start_date} till {end_date}.png') 
    
    
'''
Repair Summary per shift picture
'''
fig = plt.figure(figsize=(20, 10))

ax = fig.add_subplot(111)
plt.bar(X-0.4,+Y1_3,color = "pink", label = "Inbound", width=0.2)
plt.bar(X-0.4,-Y2_3,color = "lightgreen", label = "Outbound", width=0.2)

plt.bar(X-0.2,+Y1_1,color = "pink", width=0.2)
plt.bar(X-0.2,-Y2_1,color = "lightgreen",width=0.2)

plt.bar(X,+Y1_2,color = "pink", width=0.2)
plt.bar(X,-Y2_2,color = "lightgreen",width=0.2)

plt.bar(np.array(X1)+0.2,diff1,color = "maroon",label = "Inbound > Outbound",  width=0.2)
plt.bar(np.array(X2)+0.2,diff2,color = "olive",label = "Outbound > Inbound", width=0.2)



 
for x,y in zip(X,Y1_3):
    plt.text(x-0.4,y+0.05,'%.0f' %y, ha='center',va='bottom')
 
for x, y in zip(X, Y2_3):
    plt.text(x-0.4,-y-3,'%.0f'%y, ha='center',va='bottom')
    
for x,y in zip(X,Y1_1):
    plt.text(x-0.2,y+0.05,'%.0f' %y, ha='center',va='bottom')
 
for x, y in zip(X, Y2_1):
    plt.text(x-0.2,-y-3,'%.0f'%y, ha='center',va='bottom')
    
for x,y in zip(X,Y1_2):
    plt.text(x,y+0.05,'%.0f' %y, ha='center',va='bottom')
 
for x, y in zip(X, Y2_2):
    plt.text(x,-y-3,'%.0f'%y, ha='center',va='bottom')

for x, y in zip(X1, diff1):
    plt.text(x+0.2,y+0.05,'%.0f'%y, ha='center',va='bottom')
    
for x, y in zip(X2, diff2):
    plt.text(x+0.2,y-3,'%.0f'%-y, ha='center',va='bottom')

ax = fig.add_subplot(111)
for x in X:
    ax.annotate('3rd', xy=(x-0.45, -Y_max-15),
                horizontalalignment='left',# 注释文本的左端和低端对齐到指定位置\
                verticalalignment='bottom')

for x in X:
    ax.annotate('1st', xy=(x-0.25, -Y_max-15),
                horizontalalignment='left',# 注释文本的左端和低端对齐到指定位置\
                verticalalignment='bottom')
        
for x in X:
    ax.annotate('2nd', xy=(x-0.05, -Y_max-15),
                horizontalalignment='left',# 注释文本的左端和低端对齐到指定位置\
                verticalalignment='bottom')

for x in X:
    ax.annotate('Net', xy=(x+0.15, -Y_max-15),
                horizontalalignment='left',# 注释文本的左端和低端对齐到指定位置\
                verticalalignment='bottom')
        
        
for x in X:
    plt.hlines(-40, x-0.5, x-0.3, colors = "blue")
for x in X:
    plt.hlines(-40, x-0.3, x-0.1, colors = "blue")
for x in X:
    plt.hlines(-20, x-0.1, x+0.1, colors = "blue")
plt.xticks(X,week_date)
plt.title(f'Outbound & Inbound data per shift from {start_date} - {end_date}', fontdict={'size':20})
plt.ylabel("Amount",fontsize = 15)
plt.xlabel("Date",fontsize = 15)
plt.ylim( -Y_max-20,Y_max+20)
plt.legend(loc='upper right')
plt.show()
plt.savefig(f'./Data Summary/inbound_outbound summary/Repair Summary per shift from {start_date}till {end_date}.png') 


    
'''
Repair Difference picture
'''

# Draw plot
plt.figure(figsize=(14,10), dpi= 80)
plt.hlines(y=data.index, xmin=0, xmax=diff)
for x, y, tex in zip(diff, data.index, diff):
    t = plt.text(x, y, round(tex, 2), horizontalalignment='right' if x < 0 else 'left', 
                 verticalalignment='center', fontdict={'color':'green' if x < 0 else 'red', 'size':14})

# Decorations    
plt.yticks(data.index, week_date, fontsize=12)
plt.title(f'Daily Net Boards Coming into the Repair Station from {start_date} - {end_date}', fontdict={'size':20})
plt.xlabel("Amount",fontsize = 15)
plt.ylabel("Date",fontsize = 15)
plt.grid(linestyle='--', alpha=0.5)
plt.xlim(-1.5*max(abs(min(diff)),max(diff)), 1.5*max(abs(min(diff)),max(diff)))
plt.legend()
plt.show()
plt.savefig(f'./Data Summary/inbound_outbound summary/Net Board from {start_date} till {end_date}.png')

