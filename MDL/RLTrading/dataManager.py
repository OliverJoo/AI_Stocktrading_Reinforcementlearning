import pandas as pd
import numpy as np
import sqlalchemy
import yaml

with open('config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

# DB Connection info
DB_SECRET = _cfg['DB_SECRET']

# to see all data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 450)
pd.set_option('display.colheader_justify', 'left')

COLUMNS_CHART_DATA = ['date', 'open', 'high', 'low', 'close', 'volume']

COLUMNS_TRAINING_DATA = [
    # 'per', 'pbr', 'roe', # filtered unit by financial statement filtering but may consider them later
    'open_lastclose_ratio', 'high_close_ratio', 'low_close_ratio',
    'close_lastclose_ratio', 'volume_lastvolume_ratio',
    'close_ma5_ratio', 'volume_ma5_ratio',
    'close_ma10_ratio', 'volume_ma10_ratio',
    'close_ma20_ratio', 'volume_ma20_ratio',
    'close_ma60_ratio', 'volume_ma60_ratio',
    'close_ma120_ratio', 'volume_ma120_ratio',
    'polarity',
]


def preprocess(data):
    windows = [5, 10, 20, 60, 120]
    for window in windows:
        data[f'close_ma{window}'] = data['close'].rolling(window).mean()
        data[f'volume_ma{window}'] = data['volume'].rolling(window).mean()
        data[f'close_ma{window}_ratio'] = (data['close'] - data[f'close_ma{window}']) / data[f'close_ma{window}']
        data[f'volume_ma{window}_ratio'] = (data['volume'] - data[f'volume_ma{window}']) / data[f'volume_ma{window}']

    data['open_lastclose_ratio'] = np.zeros(len(data))
    data.loc[1:, 'open_lastclose_ratio'] = (data['open'][1:].values - data['close'][:-1].values) / data['close'][
                                                                                                   :-1].values
    data['high_close_ratio'] = (data['high'].values - data['close'].values) / data['close'].values
    data['low_close_ratio'] = (data['low'].values - data['close'].values) / data['close'].values
    data['close_lastclose_ratio'] = np.zeros(len(data))
    data.loc[1:, 'close_lastclose_ratio'] = (data['close'][1:].values - data['close'][:-1].values) / data['close'][
                                                                                                     :-1].values
    data['volume_lastvolume_ratio'] = np.zeros(len(data))
    data.loc[1:, 'volume_lastvolume_ratio'] = ((data['volume'][1:].values - data['volume'][:-1].values)
                                               / data['volume'][:-1].replace(to_replace=0, method='ffill').replace(
                to_replace=0, method='bfill').values)

    return data


def load_stock_list_from_db_pool(num_stocks):
    try:
        engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')

        sql = f'select ticker from tb_volatility_stock tvs where use_yn ="y" and tvs.ticker in (select ticker from tb_predicted_stock_pool where use_yn ="y" and weight_2 >= 0 group by ticker) order by volatility desc limit {num_stocks}'

        data = pd.read_sql(sql, engine)

    except Exception as e:
        print(e)

    return data


def load_data(ticker, date_from, date_to):
    try:
        engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')

        sql = f'select tsp.ticker as ticker, tsp.stock_name as stock_name, tsp.date as date, tsp.open as open, tsp.high as high, tsp.low as low, tsp.close as close, tsp.volume as volume,tsp.`change` as `change`, tpsp.weight_2 as polarity  from tb_stock_price tsp, tb_predicted_stock_pool tpsp where tsp.ticker = tpsp.ticker and tsp.`date`=tpsp.`date` and tsp.ticker ={ticker} and tsp.date between "{date_from}" and "{date_to}"'
        # print("sql: ", sql)
        data = pd.read_sql(sql, engine)
        data['date'] = data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        data = data.sort_values(by='date').reset_index()

        data = preprocess(data)

        data['date'] = data['date'].str.replace('-', '')
        data = data.dropna()

        # dividing chart data
        chart_data = data[COLUMNS_CHART_DATA]

        chart_data.to_csv('test1.csv')

        # dividing training data
        training_data = None
        training_data = data[COLUMNS_TRAINING_DATA]
        # training_data.to_csv('test1.csv')
        training_data = training_data.apply(np.tanh)

        # print("final : ", training_data)

        return chart_data, training_data
    except Exception as e:
        print(e)

# for test
# load_data('005930', '2002-01-02', '2022-01-16')
