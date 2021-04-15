# -*- coding: utf-8 -*-
"""
Created on Mon Mar 29 09:48:07 2021

@author: Jiaming.Zhou
"""

import pandas as pd

def ict_defect(data_path, station_data):

    data = pd.read_csv(data_path)
    station = station_data

    data = data[data['ICT Run Result'] == 'FAIL']
    data = data.merge(station, left_on = 'Board Serial Number', right_on = 'Label code')
    data['Defect name1'] = data['ICT Run Summary'].str.split('}', expand = True)[0].str.lstrip('{')
    defect_types = data[['Board Serial Number', 'Defect name1']]
    location = pd.DataFrame(data['ICT Run Summary'].str.split('}', expand = True)[1].str.split(';')).rename(columns={1:'Location'})
    location['Label code'] = data['Board Serial Number']
    location = location.explode('Location')
    location['Location'] = location['Location'].str.strip()
    location = location[location['Location'].str.len() != 0]

    def defects(df, defect, string):

        defect_df = df[df['Location'].str.contains(defect)]
        left_df = df[df['Location'].str.contains(defect) == False]

        if defect_df.shape[0] != 0:
            defect_df['Point'] = defect_df['Location'].str.split(string, expand = True)[1]
            defect_df['Label code'] = df[df['Location'].str.contains(defect)]['Label code']

        return left_df, defect_df

    left_df, components = defects(location, 'Component', 'Component ')
    left_df, devices = defects(left_df, 'Device', ' ')
    left_df, voltage = defects(left_df, 'Voltage', 'Voltage at ')
    left_df, signal = defects(left_df, 'Signal', 'Signal from ')
    left_df, boundary = defects(left_df, 'Boundary', 'Boundary Scan of ')
    left_df, open_short = defects(left_df, 'Open|Short', 'Open|Short ')

    if left_df.shape[0] == 0:

        defects = pd.concat([components, devices, voltage, signal, boundary, open_short])
        real_df = defects.merge(station, on = 'Label code')
        real_df = real_df.merge(defect_types, left_on = 'Label code', right_on = 'Board Serial Number')
        real_df = real_df[['Label code', 'Point', 'WO code', 'Product', 'Station', 'Defect name1', 'Repair', 'Week']]
        real_df = real_df.rename(columns={'Point': 'Location', 'Defect name1': 'Defect name'})
        real_df = real_df[['WO code', 'Product', 'Label code', 'Station', 'Defect name', 'Location', 'Repair', 'Week']]
        return real_df

    else:
        print('There are other types of defects')