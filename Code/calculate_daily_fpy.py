# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 11:23:52 2021

@author: Jiaming.Zhou
"""
import pandas as pd
import numpy as np
import os
def calculate_fpy_daily(pass_data, fail_data, date,station):

    data = pd.concat([pass_data, fail_data])
    #data = pass_df
    date = pd.to_datetime(date)

    data['PROCESS_COMPLETION_TIME'] = pd.to_datetime(data['PROCESS_COMPLETION_TIME'])
    data = data[data['PROCESS_COMPLETION_TIME'].dt.date == date]

    if data.shape[0] == 0:

        print(f'There is no production on {date} at {station}.')
        percent_df = pd.DataFrame()
        fpy_daily = 0
        n_pass = 0
        n = 0

    else:

        data_grouped = data.groupby('LB_ID')['PROCESS_COMPLETION_TIME'].min()
        data_grouped = pd.DataFrame(data_grouped)
        data_grouped = data_grouped.reset_index()
        data_grouped_merged = data_grouped.merge(data, on=['LB_ID', 'PROCESS_COMPLETION_TIME'])

        # Getting the number of first tests done at this station
        n = data_grouped['LB_ID'].nunique()

        # Getting the number of first tests passed at this station
        n_pass = data_grouped_merged[data_grouped_merged['IS_PASS'] == 'Y']['LB_ID'].nunique()
        fpy_daily = round(n_pass / n, 4)

        fpy_daily = str(fpy_daily * 100) + '%'

    yield_df = pd.DataFrame({'Date': date,
                             'Pass': [n_pass],
                             'Fail': [n - n_pass],
                             'FPY': [fpy_daily]})

    return yield_df



col_names = ['Date', 'Pass', 'Fail', 'FPY']

def division(x, y):
    return x/y if y else 0

def total(directory, col_names):

    result_df = pd.DataFrame(columns=col_names)

    for filename in os.listdir(directory):

        full_filename = directory + filename
        sub_df = pd.read_csv(full_filename)
        result_df = pd.concat([result_df, sub_df])
        result_df['Pass'] = result_df['Pass'].astype('int')
        result_df['Fail'] = result_df['Fail'].astype('int')

    #result_df.to_csv(f'C:/Users/Cora.Ou/Documents/Work_cora/Essential Data/2021/Data Summary/{week}/{station}_daily.csv'\
    #                 , index = False)
    pass_total = result_df['Pass'].sum()
    fail_total = result_df['Fail'].sum()
    fpy_total = round(division(pass_total, (pass_total + fail_total)), 4)
    result_df = result_df.transpose()
    result_df.columns = result_df.loc['Date']
    result_df = result_df.drop('Date')
    result_df.at['Pass','TOTAL'] = pass_total
    result_df.at['Fail', 'TOTAL'] = fail_total
    result_df.at['FPY', 'TOTAL'] = fpy_total * 100
    result_df['TOTAL'] = result_df['TOTAL'].astype('str')
    result_df.at['FPY', 'TOTAL'] = result_df.at['FPY', 'TOTAL'] + '%'

    return result_df
