import timeDelta
import yaml
import FinanceDataReader as fdr
import sqlalchemy
import pandas as pd
import pymysql
import datetime

pymysql.install_as_MySQLdb()

# DB connection info
with open(f'config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

DB_SECRET = _cfg['DB_SECRET']
VOLATILITY_TABLE = _cfg['TB_VOLATILITY']



def insert_market_stock_ohlcvc(start_date, end_date):
    """
    stock price info(OHLCVC) insert to DB
    :param start_date:str
    :param end_date:str
    :return:
    """
    markets = ['KOSPI', 'KOSDAQ']

    try:
        for market in markets:
            df_stock_list_krx = df_krx = fdr.StockListing(market)
            df_stock_list_krx.dropna(axis=0, inplace=True)
            len_df_stock_list_krx = len(df_stock_list_krx)
            print("df_stock_list_krx : ", df_stock_list_krx)
            for idx in range(0, len_df_stock_list_krx):
                print('ticker : ', df_stock_list_krx.iloc[idx]['Code'], 'Market : ',
                      df_stock_list_krx.iloc[idx]['Market'],
                      'Name : ', df_stock_list_krx.iloc[idx]['Name'])

                ticker = df_stock_list_krx.iloc[idx]['Code']
                stock_name = df_stock_list_krx.iloc[idx]['Name']

                sp_data = fdr.DataReader(ticker, start_date, end_date)
                sp_data['ticker'] = ticker
                sp_data['stock_name'] = stock_name
                sp_data['created_date'] = datetime.datetime.today()

                # print(sp_data)

                # remove trading halt list in a year (volume = 0)
                if start_date == end_date:  # prevent duplicated data
                    insert_stock_price(sp_data, 'tb_stock_price', end_date, ticker)
                else:
                    insert_stock_price(sp_data, 'tb_stock_price', 'n', ticker)
                # print(sp_data)

        # KOSPI & KOSDAQ index info update process
        # not use: unpredictable updating period of KOSPI & KOSDAQ index info
        """
        if start_date == end_date:  # prevent duplicated data
            insert_stock_price(get_kospi_kosdaq_index('KOSPI', start_date, end_date), 'tb_stock_price', end_date,
                               'KS11')
            insert_stock_price(get_kospi_kosdaq_index('KOSDAQ', start_date, end_date), 'tb_stock_price', end_date,
                               'KQ11')
        else:
            insert_stock_price(get_kospi_kosdaq_index('KOSPI', start_date, end_date), 'tb_stock_price', 'n',
                               'KS11')
            insert_stock_price(get_kospi_kosdaq_index('KOSDAQ', start_date, end_date), 'tb_stock_price', 'n',
                               'KQ11')
        """
    except Exception as e:
        print(e)


def insert_stock_price(sp_data: pd.DataFrame, db_table, del_mode, ticker):
    """
     stock price info insert
    :param sp_data: sp_data(DataFrame)
    :param db_table: db table name
    :param del_mode: n(no deleting data), end_date(deleting a specific date info)
    :param ticker: if del_mode != None, delete ticker info on end_date
    :return: no return
    """
    try:
        engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')

        if del_mode == 'n':
            # print('del mode : ', del_mode)
            sp_data.to_sql(name=db_table, con=engine, if_exists='append')
        else:
            # print('del mode : ', del_mode, ticker)
            engine.execute(f'delete from {db_table} where date = %s and ticker = %s', (del_mode, ticker))
            sp_data.to_sql(name=db_table, con=engine, if_exists='append')
    except Exception as e:
        print(e)


def volatility_stock_list(volatility: int):
    """
    updating daily average volatility info by recent 1y each stock price
    Formula : avg(high price - low price) / low price * 100 > 10
    :param: volatility:int
    :return: no return
    """
    today = datetime.date.today()
    date = today - datetime.timedelta(days=365)
    # print(today, date)

    try:
        engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')

        # usage info update - previous volatility info
        engine.execute(f'update {VOLATILITY_TABLE} set use_yn = %s', 'n')

        # over 240 trading days info only to remove trading halt list
        sql = f'select ticker, stock_name , avg((high-low)/low*100) as volatility from tb_stock_price  where DATE_FORMAT(date ,"%%Y-%%m-%%d") > "{date}" and ticker not in (select ticker from tb_stock_price where volume=0 and DATE_FORMAT(date ,"%%Y-%%m-%%d") > "{date}" group by ticker) group by ticker, stock_name having count(ticker) > 240'  # order by avg((high-low)/low*100) desc'
        print("sql: ", sql)
        df_volatility_stocks = pd.read_sql(sql, engine)

        # threshold of volatility: threshold
        df_volatility_stocks = df_volatility_stocks[df_volatility_stocks['volatility'] > volatility]
        # print("df_volatility_stocks \n", df_volatility_stocks)
        df_volatility_stocks['created_date'] = today
        df_volatility_stocks['use_yn'] = 'y'

        df_volatility_stocks.to_sql(name=VOLATILITY_TABLE, con=engine, if_exists='append', index=False)

    except Exception as e:
        print(e)


def get_kospi_kosdaq_index(index_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    get OSPI / KOSDAQ index info
    :param index_name: KOSPI or KOSDAQ
    :param start_date: index info start date
    :param end_date: index info end date
    :return: pa.DataFrame (OHLCV + Adjust Close + Ticker + Name)
    """
    if index_name == 'KOSPI':
        ticker = 'KS11'
    elif index_name == 'KOSDAQ':
        ticker = 'KQ11'

    sp_data = fdr.DataReader(ticker, start_date, end_date)
    sp_data['ticker'] = ticker
    sp_data['stock_name'] = index_name
    sp_data['created_date'] = datetime.datetime.today()
    sp_data['Change'] = 0.0
    if 'Adj Close' in sp_data.columns:
        sp_data.drop(['Adj Close'], axis=1, inplace=True)
    # print(sp_data)
    return sp_data


# Filtering function by financial statement info - debt ratio, ROA, ROE, PBR, Impaired capital, biz profit, NI, FCF, CFO
# insert filtered stock info to tradable stock db pool
# execute whenever annual financial statement updated
def insert_filtered_ticfers_to_db_pool():
    try:
        engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')
        conn = engine.connect()

        setting_date = str(timeDelta.today.year - 1) + '/12'
        # filtered stock info insert to DB Pool
        sql = f'''
        insert into tb_predicted_stock_pool(market, ticker, date, stock_name, sector, industry, weight_1, weight_2, created_date, use_yn)
                        select tab2.market as market, tab1.ticker as ticker,  tab1.date as date,tab2.stock_name as stock_name, tab2.sector as sector, tab2.industry as industry, tab2.beta as weight_1, avg(tab1.polarity) as weight_2, now(), 'y' from (select tsp.ticker as ticker, tsp.date as date, ifnull(td.polarity,0) as polarity from tb_stock_price tsp left join (select ticker, date, avg(polarity) as polarity from tb_dummy group by date) td on tsp.ticker = td.ticker and tsp.`date` =td.`date`) tab1, 
                        (select tf.market as market,tf.ticker as ticker,tf.stock_name as stock_name, tf.sector as sector, tf.industry as industry, tf.beta as beta
                        from 
                                             (select * from tb_fs where setting_date = '{setting_date}' and cf_operation > 0 and operating_profit > 0 and net_income > 0 and fcf >= 0 and total_capital > capital) tf, 
                                             (select * from tb_code tc where code_name='debt_outlier') code_debt,
                                             (select * from tb_code tc where code_name ='ROA_outlier') code_roa,
                                             (select * from tb_code tc where code_name ='ROE_outlier') code_roe,
                                             (select * from tb_code tc where code_name ='PBR_outlier') code_pbr
                        where  code_debt.code =tf.setting_date and tf.sector=code_debt.sector and tf.debt_ratio < code_debt.code_value
                        and code_roa.code =tf.setting_date and tf.sector=code_roa.sector and tf.roa < code_roa.code_value
                        and code_roe.code =tf.setting_date and tf.sector=code_roe.sector and tf.roe < code_roe.code_value
                        and code_pbr.code =tf.setting_date and tf.sector=code_pbr.sector and tf.pbr < code_pbr.code_value group by market,ticker, stock_name, sector, industry) tab2
                        where tab1.ticker =tab2.ticker 
                        group by tab2.market, tab1.ticker, tab2.stock_name, tab2.sector, tab2.industry, tab1.date, tab2.beta order by date
        '''
        engine.execute(sql)
    except Exception as e:
        print(e)
    finally:
        conn.close()


# insert_filtered_ticfers_to_db_pool()
# insert_market_stock_ohlcvc('2023-02-02', '2023-02-06')

# add input params(int)
# volatility_stock_list(2)

# print(get_kospi_kosdaq_index('KOSPI', '2022-12-23', '2022-12-28'))
# print(get_kospi_kosdaq_index('KOSDAQ', '2022-12-23', '2022-12-28'))
# insert_stock_price(get_kospi_kosdaq_index('KOSPI', '2022-12-01', '2022-12-25'), 'tb_stock_price', 'n', 'KS11')
# insert_stock_price(get_kospi_kosdaq_index('KOSDAQ', start_date, end_date), 'tb_stock_price', 'n','KQ11')

# 국고채 수익률
# print(bond.get_otc_treasury_yields("20221228"))
# print(bond.get_otc_treasury_yields("20221228", "20221228", "국민주택1종5년"))

# 한국 무위험수익률 Rm - 국고채3년
# df_Rm = bond.get_otc_treasury_yields("20221228", "20221228", "국고채3년")
# print(df_Rm)
# print(df_Rm.iloc[0]['수익률'])
