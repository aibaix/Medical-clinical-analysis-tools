import datetime
import numpy as np
import pandas as pd

from random import shuffle
from tqdm.auto import tqdm

def ReadSubtables2Merge(file_name,keywords = None):
    '''
    Read multiple sheets in the table
    And automatically find out the same fields as the key for splicing
    keywords : list of keywords
    '''
    # 读取文件
    total = pd.read_excel(file_name, sheet_name = None)
    
    # 取字段交集
    keys_list = list(set(total[list(total.keys())[0]].keys()))
    for line in ganai.keys():
        keys_list = list(set(keys_list).intersection(set(list(ganai[line].keys()))))
    
    # 检验keys是否合法
    if keywords != None:
        assert keywords in keys_list
    else:
        keywords = keys_list
    
    # 拼接
    total_merge = total[list(total.keys())[0]]
    for line in tqdm(list(total.keys())[1:]):
        total_merge = pd.merge(total_merge,total[line],on=keywords,how="outer")
    del total, keys_list, line, keywords
    total_merge.drop_duplicates(subset=keywords,keep='first',inplace=True)
    return total_merge

def Discrete2Continuous(panda_df,keyword,scale = False):
    '''
    Discrete to continuous variable
    panda_df : Table in pandas format
    keyword : Key feature
    scale : Whether to implement standardized scaling
    '''
    pp = {}
    i = 0
    for line in panda_df[keyword].values:
        if line not in pp.keys():
            pp[line] = i
            i += 1
    try:
        del pp[np.nan]
    except:
        None
    if scale:
        for line in pp.keys():
            pp[line] = pp[line]/i
    panda_df[keyword + "_values"] = panda_df[keyword].values
    panda_df[keyword + "_values"] = panda_df[keyword + "_values"].replace(pp)
    del pp,i
    # 给出修改好的结局
    return panda_df

def StandardTime2Duration(pandas_df,keyword,time_scaler = ['%Y-%m-%d']):
    '''
    Standard time to duration
    '''

    temp_date = []
    for line in pandas_df[keyword].values:
        error_num = 0
        for scaler_char in time_scaler:
            try:
                temp_date.append((datetime.datetime.strptime(line,scaler_char) - datetime.datetime.strptime("1971-1-1",'%Y-%m-%d')).days)
                break
            except:
                error_num += 1
        assert error_num < len(time_scaler) # 若报错，说明time_scaler的范围不对
    pandas_df[keyword + "_values"] = temp_date
    
    pandas_df = pandas_df.sort_values(by=[keyword + "_values"])
    timess = []
    tt = pandas_df[keyword + "_values"].values
    for i, t in enumerate(tt):
        if i == 0:
            timess.append(0)
        else:
            timess.append(t - tt[0])
    pandas_df[keyword + "_values"] = timess
    del timess, temp_date
    return pandas_df

def PatientGrouping(pandas_df,keyword):
    '''
    To group patients
    '''
    id_list = set(pandas_df[keyword].values)
    temp_dict = dict()
    for line in id_list:
        temp_dict[line] = pandas_df[pandas_df[keyword] == line]
    del id_list
    return temp_dict


def TimeSeriesFilling(pandas_df,time_sort):
    '''
    Fill by time series
    Fill forward first
    Then reverse fill
    If there is no valid value in both positive and negative directions, all zeros are filled
    '''
    pandas_df = pandas_df.sort_values(time_sort)
    pandas_df = pandas_df.fillna(method='ffill').fillna(method='bfill').fillna(0)
    return pandas_df
