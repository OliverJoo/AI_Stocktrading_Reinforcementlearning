import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import pymysql
import yaml
import sqlalchemy

pymysql.install_as_MySQLdb()

with open(f'..\..\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

# DB info
DB_SECRET = _cfg['DB_SECRET']
FS_DUMMY_TABLE = _cfg['TB_FS_DUMMY']  # test db table

def crawling_financial_statments(ticker: int):
    """
    crawling financial statements from https://finance.naver.com/
    :param ticker: ticker
    :return: no return
    """

    res = requests.get(f'https://finance.naver.com/item/coinfo.naver?code={ticker}')
    soup = BeautifulSoup(res.text, "lxml")

    stock_name = soup.select_one('.wrap_company > h2:nth-child(1) > a:nth-child(1)').text

    # iframe src
    referer = f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={ticker}'

    res = requests.get(referer)
    soup = BeautifulSoup(res.text, "lxml")

    # find href from iframe
    id_cnt = 0
    request_id = ''
    for i in soup.find('div', class_="all"):  # find all parents that have div.id
        try:
            # find id at 6th div
            if id_cnt < 6:
                request_id = i.attrs['id']
                # print("id_cnt : ", id_cnt, ", request_id:", request_id, i)
                id_cnt += 1
            else:
                break
        except Exception as e:
            # print(e)
            continue

    # print("request_id : ", request_id)

    javascript = soup.select_one('body > div > script')  # get javascript
    result = re.search("encparam", javascript.text)  # find encparam
    request_encparam = javascript.text[result.end() + 3:result.end() + 35]

    market = soup.select('dt.line-left')[8].text.split()[0]  # KOSPI or KOSDAQ
    sector = soup.select('dt.line-left')[8].text.split()[-1]  # industry classification(Main)
    industry = soup.select('dt.line-left')[9].text.split()[-1]  # industry classification(Sub)
    beta = soup.select_one(
        '#cTB11 > tbody > tr:nth-child(6) > td').text.lstrip().rstrip()  # beta, unique usage for this project

    # find cmp_cd(ticker) and encparam from javascript code(JSON type) because both params always changes
    # fin_typ=4 - IFRS linked financial statements only
    request_url = f"https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx?cmp_cd={ticker}&fin_typ=4&freq_typ=A&encparam={request_encparam}&id={request_id}"
    # print("request_url : ", request_url)

    # request headers to request with referer
    headers = {
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, bcolumnsr",
        "Accept-Language": "ko,ko-KR;q=0.9,en-US;q=0.8,en;q=0.7",
        "Host": "navercomp.wisereport.co.kr",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:107.0) Gecko/20100101 Firefox/107.0",
        "referer": referer,
    }

    res = requests.get(request_url, headers=headers)
    soup = BeautifulSoup(res.text, "lxml")

    # get published year of annual financial statements
    columns = []
    for i in soup.select('th[class^="r02c0"]')[:4]:
        column = i.text.lstrip().rstrip()[:7]
        columns.append(column)

    # get annual financial statements info
    financial_summary = soup.select('tbody')[1]

    # get index for Dataframe column
    index = []
    for i in financial_summary.select('th.bg.txt'):
        index.append(i.text.lstrip())

    df = pd.DataFrame(columns=columns, index=index)

    for idx, tr in enumerate(financial_summary.select('tr')):
        values = []
        # annual financial statement info only
        for td in tr.select('td')[:4]:
            try:
                value = td.select_one('span').text.replace(',', '')
            except Exception as e:
                value = 0
                # print(f'value error - select_one(span): ', e)
            values.append(value)

        # print("values : ", values)
        df.loc[index[idx]] = values

    df_T = df.T

    df_T['종목코드'] = ticker
    df_T['시장'] = market
    df_T['종목명'] = stock_name
    df_T['대분류'] = sector
    df_T['소분류'] = industry
    df_T['종목베타'] = beta
    df_T['폐업여부'] = 'n'  # Y - Close Biz / N - on going biz

    df_T = df_T.reset_index(drop=False)  # make published year as a column, not index
    df_T.rename(columns={'index': '결산일'}, inplace=True)

    # adjust columns for database table
    df_rename_columns = {'결산일': 'setting_date', '시장': 'market', '종목코드': 'ticker', '종목명': 'stock_name',
                         '대분류': 'sector', '소분류': 'industry', '종목베타': 'beta', '매출액': 'revenue',
                         '영업이익': 'operating_profit', '영업이익(발표기준)': 'std_operating_profit',
                         '세전계속사업이익': 'continuing_operations_profit', '당기순이익': 'net_income',
                         '당기순이익(지배)': 'control_int_net_income', '당기순이익(비지배)': 'uncontrol_int_net_income',
                         '자산총계': 'total_asset', '부채총계': 'total_debt', '자본총계': 'total_capital',
                         '자본총계(지배)': 'control_int_total_capital', '자본총계(비지배)': 'uncontrol_int_total_capital',
                         '자본금': 'capital', '영업활동현금흐름': 'cf_operation', '투자활동현금흐름': 'cf_investing',
                         '재무활동현금흐름': 'cf_financing', 'CAPEX': 'capex', 'FCF': 'fcf', '이자발생부채': 'debt_from_int',
                         '영업이익률': 'operating_margin', '순이익률': 'net_margin', 'ROE(%)': 'roe', 'ROA(%)': 'roa',
                         '부채비율': 'debt_ratio', '자본유보율': 'retention_rate', 'EPS(원)': 'eps', 'PER(배)': 'per',
                         'BPS(원)': 'bps', 'PBR(배)': 'pbr', '현금DPS(원)': 'cash_dps', '현금배당수익률': 'cash_div_return',
                         '현금배당성향(%)': 'cash_div_payout_ratio', '발행주식수(보통주)': 'issued_shares', '폐업여부': 'closure_yn'
                         }

    df_T.rename(columns=df_rename_columns, inplace=True)

    # DB insert
    try:
        engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')
        df_T.to_sql(name=FS_DUMMY_TABLE, con=engine, if_exists='append', index=False)
        print(f"{stock_name}'s financial statement info insert into {FS_DUMMY_TABLE}")
    except Exception as e:
        print(f'DB insert exception({FS_DUMMY_TABLE}): ', e)

# test
crawling_financial_statments('214870')
