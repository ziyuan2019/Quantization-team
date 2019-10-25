import pandas as pd
import numpy as np
import talib

def data_get(path):
    '''
    获取从数据库下载的原始数据并作简单的处理
    :param path: str
    :return: datafram
    '''
    data = []
    for x in range(1, 8):
        df = path+'TRD_Dalyr{}.txt'.format(x)
        with open(df, encoding='utf_8') as f:
            lines= f.readlines()[1:]
            for line in lines:
                data.append(line.split())
    columns = ['Stkcd', 'Trddt', 'Opnprc', 'Hiprc', 'Loprc', 'Clsprc', 'Dnshrtrd', 'Dnvaltrd', 'Dsmvosd', 'Markettype']
    data = pd.DataFrame(data, columns=columns).sort_values(by='Trddt').reset_index(drop=True)
    data['﻿Stkcd'] = data['Stkcd'].astype('str')
    data['Trddt'] = data['Trddt'].astype('str')
    drop_columns = ['Stkcd', 'Trddt']
    for i in list(set(columns) - set(drop_columns)):
        data[i] = data[i].astype(np.float64)
    data['Tnosic'] = np.true_divide(np.array(data['Dsmvosd']), np.array(data['Clsprc'])) * 1000
    data['Tnosic'] = data['Tnosic'].astype('int64')
    return data


def turnover_compute(data):
    data['turnover'] = np.true_divide(np.array(data['Dnshrtrd']), np.array(data['Tnosic']))
    data['Trddt_month'] = [i[0:7] for i in data['Trddt']]
    data_grouped = data.groupby(['Trddt_month', 'Stkcd'], as_index=False)['turnover'].mean()
    data_grouped = data_grouped.rename(columns={'turnover': 'turnover_month'})
    data = pd.merge(data, data_grouped, how='left', left_on=['Trddt_month', 'Stkcd'], right_on=['Trddt_month', 'Stkcd'])
    return data


def index_ma_compute(data):
    data['MA_5'] = talib.MA(data.Clsprc,timeperiod=5,matype=0)
    data['MA_10'] = talib.MA(data.Clsprc,timeperiod=10,matype=0)
    data['MA_20'] = talib.MA(data.Clsprc,timeperiod=20,matype=0)
    data['MA_60'] = talib.MA(data.Clsprc,timeperiod=60,matype=0)
    return data


def get_month_data(data):
    data = data.sort_values(['Trddt', 'Stkcd']).reset_index(drop=True)
    data_grouped = data.groupby(['Stkcd', 'Trddt_month']).apply(lambda x: x.iloc[len(x)-1])
    data_grouped = data_grouped.reset_index(drop=True)
    return data_grouped


if __name__=='__main__':
    path = "C:/others/量化/market_data/"
    data = data_get(path)
    data = turnover_compute(data)
    data = index_ma_compute(data)
    data_month = get_month_data(data)
    data_month.to_csv('C:/others/量化/market_data/turnover_data.txt', sep='\t', index=False)
