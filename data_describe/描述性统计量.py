# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 14:31:01 2019

@author: lizi
描述性统计量
"""

import pandas as pd
import numpy as np

data_file='oppo_newfeature_20190909m.csv'
version='20190909'
# data_file = 'file:///D:/lizi/data_feature_final_oppo_20190909_check.csv'
# data_file='/var/mobstazdata/jenkins/workspace/pipeline-jenkins/backtracking_feature_pipeline_20190301_new/oppo_20190909/final/data_feature_final_oppo_20190912.csv'
usertable = pd.read_csv(data_file)

# usertable_test = usertable.head()

# usertable_test1 = usertable[usertable_test.columns[0:10]]



def decribe_func(data):
    columns=['feature','样本总量','0值总量','na值总量','-9999值总量','9999值总量','查得率','查得总量','查得均值','查得方差','查得最小值','查得25%位数','查得50%位数','查得75%位数','查得最大值']
    descirbe_final = []
    for i in data.columns:
        temp = data[i]
        sum_total = len(temp)
#        sum_na = temp[temp != temp].count()
        sum_na = len(temp[temp != temp])
        sum_zero = temp[temp==0].count()
        sum_fillna_9999_ = temp[temp==-9999].count()
        sum_fillna_9999 = temp[temp==9999].count()
        temp_drop = temp.dropna()[temp != -9999][temp != 9999][temp != 0]
        temp_drop_descirbe = temp_drop.describe()
        describe_final_temp = [i,sum_total,sum_zero,sum_na,sum_fillna_9999_,sum_fillna_9999,temp_drop_descirbe['count']/sum_total] + list(temp_drop_descirbe) 
        descirbe_final.append(describe_final_temp)
    return pd.DataFrame(descirbe_final,columns=columns)

usertable_describe = decribe_func(usertable)

usertable_describe.to_csv('usertable_describe_'+version+'.csv',index=False)
