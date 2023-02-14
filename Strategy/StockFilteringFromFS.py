import timeDelta
import yaml
import sqlalchemy


# DB connection info
with open(f'..\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

DB_SECRET = _cfg['DB_SECRET']
VOLATILITY_TABLE = _cfg['TB_VOLATILITY']


# Filtering function by financial statement info - debt ratio, ROA, ROE, PBR, Impaired capital, biz profit, NI, FCF, CFO
# insert filtered stock info to tradable stock db pool
# execute whenever annual financial statement updated
def insert_filtered_tickers_to_db_pool():
    try:
        engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')

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
