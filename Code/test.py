# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 10:54:37 2021

@author: Jiaming.Zhou
"""
import cx_Oracle #导入包
dsnStr = cx_Oracle.makedsn("10.20.193.53", "1521", "IMSDBPROD")
db = cx_Oracle.connect('xiaoma','foxconn123MKE',dsn=dsnStr) #连接数据库
print(db.version) #打印版本看看 显示 11.2.0.1.0
cur = db.cursor() # 游标操作
cur.execute('select * from table') # 执行sql语句
rows = cur.fetchall() # 获取数据
# 打印数据
for row in rows[:10]:
   print(f"{row[0]} ,",end='')