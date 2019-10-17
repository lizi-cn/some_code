"""
将回溯数据合并,并检查准确性,填充缺失值
"""

import pandas as pd
import sys

version = sys.argv[1]


df = pd.read_csv(version+'/data_feature_'+version+'_header', dtype={'imei_ori':str, 'dayno':int})

app_install_7 = pd.read_csv(version+'/final'+'/data_app_install_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_install_15 = pd.read_csv(version+'/final'+'/data_app_install_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_install_30 = pd.read_csv(version+'/final'+'/data_app_install_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_install_90 = pd.read_csv(version+'/final'+'/data_app_install_'+version+'_'+'90'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})

app_uninstall_7 = pd.read_csv(version+'/final'+'/data_app_uninstall_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_uninstall_15 = pd.read_csv(version+'/final'+'/data_app_uninstall_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_uninstall_30 = pd.read_csv(version+'/final'+'/data_app_uninstall_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_uninstall_90 = pd.read_csv(version+'/final'+'/data_app_uninstall_'+version+'_'+'90'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})

app_usage_7 = pd.read_csv(version+'/final'+'/data_app_usage_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_usage_15 = pd.read_csv(version+'/final'+'/data_app_usage_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_usage_30 = pd.read_csv(version+'/final'+'/data_app_usage_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_usage_90 = pd.read_csv(version+'/final'+'/data_app_usage_'+version+'_'+'90'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})

app_usage_ratio_7 = pd.read_csv(version+'/final'+'/data_app_usage_ratio_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_usage_ratio_15 = pd.read_csv(version+'/final'+'/data_app_usage_ratio_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_usage_ratio_30 = pd.read_csv(version+'/final'+'/data_app_usage_ratio_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
app_usage_ratio_90 = pd.read_csv(version+'/final'+'/data_app_usage_ratio_'+version+'_'+'90'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})


data_app_usage_stats_by_app_dayno_7 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_app_dayno_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_app_usage_stats_by_app_dayno_15 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_app_dayno_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_app_usage_stats_by_app_dayno_30 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_app_dayno_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_app_usage_stats_by_app_dayno_90 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_app_dayno_'+version+'_'+'90'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})

data_app_usage_stats_by_app_7 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_app_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_app_usage_stats_by_app_15 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_app_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_app_usage_stats_by_app_30 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_app_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_app_usage_stats_by_app_90 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_app_'+version+'_'+'90'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})

data_app_usage_stats_by_dayno_7 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_dayno_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_app_usage_stats_by_dayno_15 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_dayno_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_app_usage_stats_by_dayno_30 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_dayno_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_app_usage_stats_by_dayno_90 = pd.read_csv(version+'/final'+'/data_app_usage_stats_by_dayno_'+version+'_'+'90'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})


app_recent_usage_days_diff_90 =  pd.read_csv(version+'/final'+'/data_app_recent_usage_days_diff_'+version+'_'+'90'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})

search_keyword_count_7 = pd.read_csv(version+'/final'+'/data_search_keyword_count_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
search_keyword_count_15 = pd.read_csv(version+'/final'+'/data_search_keyword_count_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
search_keyword_count_30 = pd.read_csv(version+'/final'+'/data_search_keyword_count_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})

search_keyword_count_ratio_7 = pd.read_csv(version+'/final'+'/data_search_keyword_count_ratio_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
search_keyword_count_ratio_15 = pd.read_csv(version+'/final'+'/data_search_keyword_count_ratio_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
search_keyword_count_ratio_30 = pd.read_csv(version+'/final'+'/data_search_keyword_count_ratio_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})


app_em_7 = pd.read_csv(version+'/data_appem_50dim_'+'7'+'days_'+version+'_longterm_20181018_longer_50dim', dtype={'imei_ori':str, 'dayno':int},names=['imei_ori']+['app_em_7_'+str(i+1) for i in range(50)]+['dayno'])
app_em_15 = pd.read_csv(version+'/data_appem_50dim_'+'15'+'days_'+version+'_longterm_20181018_longer_50dim', dtype={'imei_ori':str, 'dayno':int},names=['imei_ori']+['app_em_15_'+str(i+1) for i in range(50)]+['dayno'])
app_em_30 = pd.read_csv(version+'/data_appem_50dim_'+'30'+'days_'+version+'_longterm_20181018_longer_50dim', dtype={'imei_ori':str, 'dayno':int},names=['imei_ori']+['app_em_30_'+str(i+1) for i in range(50)]+['dayno'])

app_em_7_2 = pd.read_csv(version+'/data_appem_50dim_'+'7'+'days_'+version+'_longterm_20180523_50dim', dtype={'imei_ori':str, 'dayno':int},names=['imei_ori']+['app_em_7_2_'+str(i+1) for i in range(50)]+['dayno'])
app_em_15_2 = pd.read_csv(version+'/data_appem_50dim_'+'15'+'days_'+version+'_longterm_20180523_50dim', dtype={'imei_ori':str, 'dayno':int},names=['imei_ori']+['app_em_15_2_'+str(i+1) for i in range(50)]+['dayno'])
app_em_30_2 = pd.read_csv(version+'/data_appem_50dim_'+'30'+'days_'+version+'_longterm_20180523_50dim', dtype={'imei_ori':str, 'dayno':int},names=['imei_ori']+['app_em_30_2_'+str(i+1) for i in range(50)]+['dayno'])


wifi_max= pd.read_csv(version+'/final'+'/data_wifi_features_'+version+'.csv', dtype={'imei_ori':str, 'dayno':int})


geohash_max_count_ratio_30 = pd.read_csv(version+'/final'+'/data_geohash_features_max_count_ratio_'+version+'_'+'30'+'_day'+'.csv', dtype={'imei_ori':str, 'dayno':int})
geohash_max_count_ratio_60 = pd.read_csv(version+'/final'+'/data_geohash_features_max_count_ratio_'+version+'_'+'60'+'_day'+'.csv', dtype={'imei_ori':str, 'dayno':int})
geohash_max_count_ratio_90 = pd.read_csv(version+'/final'+'/data_geohash_features_max_count_ratio_'+version+'_'+'90'+'_day'+'.csv', dtype={'imei_ori':str, 'dayno':int})


data_rom_ratio_7 = pd.read_csv(version+'/final'+'/data_rom_ratio_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_rom_ratio_15 = pd.read_csv(version+'/final'+'/data_rom_ratio_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_rom_ratio_30 = pd.read_csv(version+'/final'+'/data_rom_ratio_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})


data_city_count_7 = pd.read_csv(version+'/final'+'/data_city_count_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_city_count_15 = pd.read_csv(version+'/final'+'/data_city_count_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_city_count_30 = pd.read_csv(version+'/final'+'/data_city_count_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})


data_sim_count_7 = pd.read_csv(version+'/final'+'/data_sim_count_'+version+'_'+'7'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_sim_count_15 = pd.read_csv(version+'/final'+'/data_sim_count_'+version+'_'+'15'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_sim_count_21 = pd.read_csv(version+'/final'+'/data_sim_count_'+version+'_'+'21'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})
data_sim_count_30 = pd.read_csv(version+'/final'+'/data_sim_count_'+version+'_'+'30'+'_day_pivot'+'.csv', dtype={'imei_ori':str, 'dayno':int})


wifi_7 = pd.read_csv(version+'/final'+'/data_wifi_count_'+version+'_midnight'+'_'+'7'+'_day'+'.csv', dtype={'imei_ori':str, 'dayno':int})
wifi_15 = pd.read_csv(version+'/final'+'/data_wifi_count_'+version+'_midnight'+'_'+'15'+'_day'+'.csv', dtype={'imei_ori':str, 'dayno':int})
wifi_21 = pd.read_csv(version+'/final'+'/data_wifi_count_'+version+'_midnight'+'_'+'21'+'_day'+'.csv', dtype={'imei_ori':str, 'dayno':int})
wifi_30 = pd.read_csv(version+'/final'+'/data_wifi_count_'+version+'_midnight'+'_'+'30'+'_day'+'.csv', dtype={'imei_ori':str, 'dayno':int})



from functools import reduce
dfs = [df,
       app_install_7,
       app_install_15,
       app_install_30,
       app_install_90,
       app_uninstall_7,
       app_uninstall_15,
       app_uninstall_30,
       app_uninstall_90,
       app_usage_ratio_7,
       app_usage_ratio_15,
       app_usage_ratio_30,
       app_usage_ratio_90,
       app_usage_7,
       app_usage_15,
       app_usage_30,
       app_usage_90,
       data_app_usage_stats_by_app_dayno_7,
       data_app_usage_stats_by_app_dayno_15,
       data_app_usage_stats_by_app_dayno_30,
       data_app_usage_stats_by_app_dayno_90,
       data_app_usage_stats_by_app_7,
       data_app_usage_stats_by_app_15,
       data_app_usage_stats_by_app_30,
       data_app_usage_stats_by_app_90,
       data_app_usage_stats_by_dayno_7,
       data_app_usage_stats_by_dayno_15,
       data_app_usage_stats_by_dayno_30,
       data_app_usage_stats_by_dayno_90,
       app_recent_usage_days_diff_90,
       search_keyword_count_7,
       search_keyword_count_15,
       search_keyword_count_30,
       search_keyword_count_ratio_7,
       search_keyword_count_ratio_15,
       search_keyword_count_ratio_30,
       wifi_max,
       geohash_max_count_ratio_30,
       geohash_max_count_ratio_60,
       geohash_max_count_ratio_90,
       data_rom_ratio_7,
       data_rom_ratio_15,
       data_rom_ratio_30,
       data_city_count_7,
       data_city_count_15,
       data_city_count_30,
       data_sim_count_7,
       data_sim_count_15,
       data_sim_count_21,
       data_sim_count_30,
       wifi_7,
       wifi_15,
       wifi_21,
       wifi_30,
       app_em_7,
       app_em_15,
       app_em_30,
       app_em_7_2,
       app_em_15_2,
       app_em_30_2
       ]

df_final = reduce(lambda left,right: pd.merge(left,right,on=['imei_ori','dayno'],how='left'), dfs)




df_check_nan = df_final.isnull().sum().reset_index()
df_check_nan.columns = ['feature', 'nan_cnt']

df_check_nan['not_nan_cnt'] = df_final.shape[0]-df_check_nan.nan_cnt

df_check_nan[df_check_nan.nan_cnt==df_final.shape[0]]


14365 13344 1021 
#### fill na
df_final['phone_price_price'] = (df_final['phone_price_price']-2288.0)/895.01066

df_final[[i for i in df_final.columns if 'days_diff_' in i]] = df_final[[i for i in df_final.columns if 'days_diff_' in i]].fillna(0)


df_final[[i for i in df_final.columns if 'app_em' not in i]] = df_final[[i for i in df_final.columns if 'app_em' not in i]].fillna(0) 

df_final[df_final['app_sum_all_90_app_count']==0] = df_final[df_final['app_sum_all_90_app_count']==0].replace(0,np.nan)



#### rm problem_imei
# df_final.imei_version.value_counts()
df_final = df_final[df_final.problem_imei!=1]


# df_final[df_final.problem_imei_given==1].shape
# df_final[df_final.problem_imei_given_md5==1].shape



z = df_final[['imei_ori','imei','imei_version']].drop_duplicates().imei.value_counts().reset_index().rename(columns={'index':'imei','imei':'imei_cnt'})

multi_imei_list = z[z.imei_cnt>1]

df_final_single = df_final[~df_final.imei.isin(multi_imei_list.imei)].drop(['imei_version','imei_ori'], axis=1)

df_final_multi = df_final[df_final.imei.isin(multi_imei_list.imei)]

# df_final_multi[((df_final_multi.imei_version!='imei') & (df_final_multi.imei_version!='imei_md5'))][['imei_ori','imei','imei_version']].head()

### remove app_sum_all_90_start_times<=0

df_final_multi = df_final_multi[((df_final_multi.imei_version!='imei') & (df_final_multi.imei_version!='imei_md5')) & (df_final_multi.app_sum_all_90_start_times>0) & (~df_final_multi.imei_ori.isin(['PACM00','PADM00','PACT00','PADT00']))]

zz = df_final_multi[['imei_ori','imei','imei_version']].drop_duplicates().imei.value_counts().reset_index().rename(columns={'index':'imei','imei':'imei_cnt'})

multi_imei_list2 = zz[zz.imei_cnt>1]

df_final_multi1 = df_final_multi[~df_final_multi.imei.isin(multi_imei_list2.imei)]


### remove still multi-result
df_final_multi2 = df_final_multi[df_final_multi.imei.isin(multi_imei_list2.imei)]

df_final_multi2_choose = df_final_multi2.groupby(['imei', 'dayno']).apply(lambda x:x.imei_ori.max()).reset_index().rename(columns={0:'imei_ori'})

df_final_multi2 = pd.merge(df_final_multi2, df_final_multi2_choose, on=['imei', 'imei_ori','dayno'])

df_final_output = pd.concat([df_final_single, df_final_multi1, df_final_multi2])



df_final_output.drop(['imei_ori','imei_version'],axis=1).to_csv(version+'/final'+'/data_feature_final_'+version+'.csv',index=False)

