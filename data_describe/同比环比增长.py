# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 09:54:04 2019

@author: lizi
"""
"""
计算同比增长率和环比增长率
"""
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import sys


# =============================================================================
# input param 
data_file='file:///C:/Users/lizi/Desktop/futu/futu_mau.csv'
columns=['pack_name','app_name','mau','dayno','version']
# 
# =============================================================================
 
def table_stander(datafile,columns):
#    DataFrame标准化处理 及相应的数据检查
    df_read = pd.read_csv(data_file,header=None,names=columns)
    df_check_unique = df_read.pivot_table(values='mau',index='dayno',columns='app_name',aggfunc='count')
    if sum(df_check_unique.values>1).max()>0:
        print('error:index is not unique')    
#        报错后,检查原始数据是否唯一 
        sys.exit()
    df_pivot = df_read.pivot_table(values='mau',index='dayno',columns='app_name',aggfunc='mean') 
    return df_pivot

#tongbi
def statics_tongbi(df):
#    df是Dataframe,index是日期,列是各个app 
#    判断index是年月还是年月日 
    df = df.sort_index(axis=0,ascending=True).sort_index(axis=1,ascending=True)
    if len(str(df.index[0]))==6:
        df['column_']=df.index.map(lambda x:int((pd.to_datetime(str(x),format='%Y%m')+relativedelta(years=1)).strftime('%Y%m')))
        print(1)
    elif len(str(df.index[0]))==8:
        df['column_']=df.index.map(lambda x:int((pd.to_datetime(str(x),format='%Y%m%d')+relativedelta(years=1)).strftime('%Y%m%d')))
        print(2)
    elif len(str(df.index[0]))==4:
        df['column_']=df.index.map(lambda x:int((pd.to_datetime(str(x),format='%Y')+relativedelta(years=1)).strftime('%Y')))
        print(3)
    else:
        print('check usertable_pivot`s index')
#        报错后,检查index的格式 
        sys.exit()
    
    date_index = df['column_'][df['column_'].isin(df.index)]
    df_fenmu = df.loc[date_index.index].set_index(keys='column_',drop=True)
    df_fenzi = df.loc[date_index].drop('column_',axis=1)   
#    计算同比 
    df_final = (df_fenzi/df_fenmu).applymap(lambda x:x-1)
    return df_final 

#huanbi
def statics_huanbi(df):
    df = df.sort_index(axis=0,ascending=True).sort_index(axis=1,ascending=True)
    df_diff = df.diff().iloc[1:]
    df_denominator = df.iloc[:-1]
    df_denominator.index = df.index[1:]
    df_huanbi = df_diff/df_denominator 
    return df_huanbi    
    

def report_output():
    usertable_pivot = table_stander(data_file,columns)  
    now = pd.datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_writer = pd.ExcelWriter('C:/Users/lizi/Desktop/report(tongbi_huanbi)_' + now + '.xlsx')
    statics_tongbi(usertable_pivot).to_excel(excel_writer,sheet_name = 'tongbi')
    statics_huanbi(usertable_pivot).to_excel(excel_writer,sheet_name = 'huanbi')
    excel_writer.save()
    excel_writer.close()

if __name__ == "__main__":
    report_output()
    
    

