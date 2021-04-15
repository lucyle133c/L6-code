# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 11:20:52 2021

@author: Jiaming.Zhou

Program Summary
Purpose: Calculate FT/BSI station FPY 
Inputs: FT For FPY Calculation Usage only.csv/BSI For FPY Calculation Usage only.csv
Outputs: FT/BSI PASS/FAIL amount(JZ 04142021)
Side Effects : Not accurate engough(haven't consider the situation that the board exist in both result)
"""
import pandas as pd

week = 'WK10'
data = pd.read_csv(f"C:/JZhou/L6/{week}/{week}/FT/FT For FPY Calculation Usage only.csv")

data.groupby('FT Run Result')['Board Serial Number'].nunique()

data1 = pd.read_csv(f"C:/JZhou/L6/{week}/{week}/BSI/BSI For FPY Calculation Usage only.csv")

data1.groupby('BSI Run Result')['Board Serial Number'].nunique()

