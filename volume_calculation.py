# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 08:15:50 2021

@author: Jiaming.Zhou
"""

import datetime
import pandas as pd
import numpy as np


def throughput(data, bsi_path, ft_path, savepath, start_date, end_date, start_dt2, end_dt2):

    bsi = pd.read_csv(bsi_path)
    ft = pd.read_csv(ft_path)

    bsi = bsi[bsi['Golden Board?'] == False]
    ft = ft[ft['Golden Board?'] == False]

    bsi = bsi[['Board Serial Number', 'BSI Run SortableTime', 'BSI Run Result']]
    bsi = bsi.rename(columns={'Board Serial Number': 'LB_ID',
                              'BSI Run Result': 'IS_PASS',
                              'BSI Run SortableTime': 'WP_CMP_DATE'})
    bsi['WP_ID'] = 'BSI'
    bsi['WP_CMP_DATE'] = pd.to_datetime(
        bsi['WP_CMP_DATE'], format='%Y/%m/%d,%H:%M:%S')
    bsi = bsi[(bsi['WP_CMP_DATE'] <= end_dt2) &
              (bsi['WP_CMP_DATE'] >= start_dt2)]

    ft = ft[['Board Serial Number', 'FT Run SortableTime',
             'FT Station ID', 'FT Run Result']]
    ft = ft.rename(columns={'Board Serial Number': 'LB_ID',
                            'FT Run Result': 'IS_PASS',
                            'FT Run SortableTime': 'WP_CMP_DATE'})
    ft['WP_ID'] = np.where(
        ft['FT Station ID'].str.contains('FT1'), 'FT1', 'FT2')
    ft = ft[['LB_ID', 'WP_ID', 'IS_PASS', 'WP_CMP_DATE']]

    ft['WP_CMP_DATE'] = pd.to_datetime(
        ft['WP_CMP_DATE'], format='%Y/%m/%d,%H:%M:%S')
    ft = ft[(ft['WP_CMP_DATE'] <= end_dt2) & (ft['WP_CMP_DATE'] >= start_dt2)]

    data = data[data['WP_ID'].isin(['PTH-BSI', 'PTH-FT1', 'PTH-FT2']) == False]
    mapping_dict = \
        {'Process009': 'BOT',
         'Process019': 'TOP',
         'PTH-VI': 'VI',
         'PTH-OST': 'OST',
         'PTH-ICT': 'ICT',
         # 'PTH-BSI': 'BSI',
         # 'PTH-FT1': 'FT1',
         'PTH-Packing': 'PACK'
         }

    data['WP_ID'] = data['WP_ID'].map(mapping_dict).fillna(data['WP_ID'])

    mapping_dict1 = \
        {'Y': 'PASS',
         'N': 'FAIL'
         }

    data['IS_PASS'] = data['IS_PASS'].map(mapping_dict1)
    data = pd.concat([data, bsi, ft])

    data['WP_CMP_DATE'] = pd.to_datetime(data['WP_CMP_DATE'])
    data['hour'] = data['WP_CMP_DATE'].dt.hour
    data['Date'] = data['WP_CMP_DATE'].dt.date
    data['Shift'] = np.where(data['hour'].isin([0, 1, 2, 3, 4, 5, 6]), '3rd Shift',
                             np.where(data['hour'].isin([7, 8, 9, 10, 11, 12, 13, 14]), '1st Shift', '2nd Shift'))
    # data['Date'] = np.where(data['Shift'] == '3rd Shift', data['Date'] - pd.Timedelta(days=1),data['Date'])

    # Calculate per day, per shift pass/fail
    data_grouped = data.groupby(['Date', 'Shift', 'WP_ID', 'IS_PASS'])[
        'LB_ID'].nunique().reset_index()
    data_grouped = data_grouped.rename(columns={'WP_ID': 'Station',
                                                'IS_PASS': 'Result',
                                                'LB_ID': 'Count'})
    data_grouped['Date'] = pd.to_datetime(data_grouped['Date'])

    mux = pd.MultiIndex.from_product([data_grouped['Date'].unique(), data_grouped['Shift'].unique(),
                                      data_grouped['Station'].unique(), data_grouped['Result'].unique()],
                                     names=('Date', 'Shift', 'Station', 'Result')).to_frame().reset_index(drop=True)
    data_grouped = mux.merge(
        data_grouped, on=['Date', 'Shift', 'Station', 'Result'], how='outer').fillna(0)

    # Calculate per day, per shift total
    data_grouped1 = data_grouped.groupby(['Date', 'Shift', 'Station'])[
        'Count'].sum().reset_index()
    data_grouped1['Result'] = 'Total'
    data_grouped1 = data_grouped1[[
        'Date', 'Shift', 'Station', 'Result', 'Count']]

    shift_summary = pd.concat([data_grouped, data_grouped1])
    shift_summary['Station'] = pd.Categorical(
        shift_summary['Station'],
        categories=['BOT', 'TOP', 'VI', 'OST',
                    'ICT', 'BSI', 'FT1', 'FT2', 'PACK'],
        ordered=True
    )

    shift_summary['Shift'] = pd.Categorical(
        shift_summary['Shift'],
        categories=['3rd Shift', '1st Shift', '2nd Shift', 'All'],
        ordered=True
    )

    shift_summary['Result'] = pd.Categorical(
        shift_summary['Result'],
        categories=['FAIL', 'PASS', 'Total'],
        ordered=True
    )

    shift_summary = shift_summary.sort_values(
        by=['Date', 'Station', 'Shift', 'Result'])

    # Calculate per day pass/fail
    data_grouped2 = shift_summary.groupby(['Date', 'Station', 'Result'])[
        'Count'].sum().reset_index()
    data_grouped2['Shift'] = 'All'
    all1 = pd.concat([shift_summary, data_grouped2])

    all1['Station'] = pd.Categorical(
        all1['Station'],
        categories=['BOT', 'TOP', 'VI', 'OST',
                    'ICT', 'BSI', 'FT1', 'FT2', 'PACK'],
        ordered=True
    )

    all1['Shift'] = pd.Categorical(
        all1['Shift'],
        categories=['3rd Shift', '1st Shift', '2nd Shift', 'All'],
        ordered=True
    )

    all1['Result'] = pd.Categorical(
        all1['Result'],
        categories=['FAIL', 'PASS', 'Total'],
        ordered=True
    )

    all1 = all1.sort_values(by=['Date', 'Station', 'Shift', 'Result'])
    all1['div'] = all1.groupby(['Date', 'Shift', 'Station'])['Count'].shift(1)
    all1['fpy'] = round(all1['div'] / all1['Count'], 4)
    all1['fpy'] = np.where(all1['Result'] == 'Total', all1['fpy'], 0)
    all2 = all1[all1['Result'] == 'Total'][['Date', 'Shift', 'Station', 'fpy']]
    all2 = all2.rename(columns={'fpy': 'Count'})
    all2['Result'] = 'Fpy'
    all2['Count'] = all2['Count'].fillna(0)
    all2 = all2[['Date', 'Shift', 'Station', 'Result', 'Count']]
    all1 = all1[['Date', 'Shift', 'Station', 'Result', 'Count']]
    all1 = pd.concat([all1, all2])

    all1['Result'] = pd.Categorical(
        all1['Result'],
        categories=['FAIL', 'PASS', 'Total', 'Fpy'],
        ordered=True
    )

    all1 = all1.sort_values(by=['Date', 'Station', 'Shift', 'Result'])
    all1 = all1[all1['Result'].isna() == False]
    #fpy_daily = all1[all1['Result'] == 'Fpy']
    #fpy_daily.to_csv('C:/Users/Cora.Ou/Documents/Work_cora/Essential Data/2021/Data Summary/fpy daily/fpy_daily.csv')
    all1 = all1.pivot(index=['Date', 'Shift', 'Result'],
                      columns='Station', values='Count')
    all1.to_csv(savepath)
