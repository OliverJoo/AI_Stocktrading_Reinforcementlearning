import pandas as pd
from tqdm import tqdm
import datetime
import yaml
import sqlalchemy
import snscrape.modules.twitter as sntwitter
from multiprocessing import Pool

# DB connection info
with open(f'..\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

DB_SECRET = _cfg['DB_SECRET']
CONTENTS_TABLE = _cfg['TB_CONTENTS']
TRADABLE_STOCK_LIST = _cfg['GET_TRADABLE_STOCK_LIST']


def mainProcess():
    try:
        with Pool(6) as p:
            array_list = []

            array_list.append(['MultiProcess 1', '2023-01-01', '2023-02-05'])
            # array_list.append(['MultiProcess 2', '2021-12-31', '2022-12-31'])
            # array_list.append(['MultiProcess 3', '2019-01-01', '2020-01-01'])
            # array_list.append(['MultiProcess 4', '2020-01-01', '2021-01-01'])
            # array_list.append(['MultiProcess 5', '2021-01-01', '2022-01-01'])
            # array_list.append(['MultiProcess 6', '2021-12-31', '2022-12-31'])

            p.map(multiProcess, array_list)

    except Exception as e:
        print(e)


def multiProcess(array_list):
    try:
        # get parameters
        process_name = array_list[0]
        start_date = array_list[1]
        end_date = array_list[2]
        # print(process_name, start_date, end_date)

        # get tradable stock list from DB
        engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')
        filtered_stock_list = pd.read_sql(TRADABLE_STOCK_LIST, engine)

        start_num, len_filtered_stock_list = 0, len(filtered_stock_list)

        tweet_list = []
        for idx in tqdm(range(start_num, len_filtered_stock_list)):
            ticker = filtered_stock_list.iloc[idx]['ticker']
            stock_name = filtered_stock_list.iloc[idx]['stock_name']
            market = filtered_stock_list.iloc[idx]['market']

            # print("for start : ", ticker, stock_name, market)

            # set search word and term
            search_query = stock_name + ' since:' + start_date + ' until:' + end_date + ' near:"Seoul" within:400km'
            # print("search_query : ", search_query)

            for tweet in sntwitter.TwitterSearchScraper(search_query).get_items():
                # except for retweet and less than 10 words
                if str(tweet.content).startswith('@') or len(tweet.content) < 10:
                    continue
                else:
                    tweet_list.append([ticker, stock_name, tweet.date, tweet.content])

        market_df = pd.DataFrame(tweet_list, columns=['ticker', 'stock_name', 'published_date', 'contents'])
        market_df['created_date'] = datetime.datetime.today()
        market_df['use_yn'] = 'y'
        market_df['identity'] = 'twitter'
        # market_df.to_csv(f'{market}_{start_date}.csv')
        market_df.to_sql(name=CONTENTS_TABLE, con=engine, if_exists='append', index=False)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    mainProcess()
