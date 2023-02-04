# execute crawling : scrapy crawl newsspider or execute main.py
# for log: scrapy crawl newsspider > temp_log.txt
import scrapy
import time
import yaml
import datetime
import requests
from bs4 import BeautifulSoup
import sqlalchemy
import pymysql
import pandas as pd
import random
from newsSpider.items import NewsspiderItem

pymysql.install_as_MySQLdb()

# DB connection info
with open(f'..\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

DB_SECRET = _cfg['DB_SECRET']

# dummy user agent list
"""
user_agent_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1',
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36 Edg/87.0.664.75',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18363',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1',
    'Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6',
    'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5',
    'Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3',
    'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24',
    'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24'
]
"""

base_url = 'https://search.naver.com/'


class NewsspiderSpider(scrapy.Spider):
    name = 'newsspider'
    allowed_domains = ['search.naver.com', 'www.mk.co.kr', 'www.hankyung.com', 'www.fnnews.com', 'news.heraldcorp.com','biz.heraldcorp.com']

    def start_requests(self):

        try:
            press_lists = ['1015', '1009', '1014', '1016']  # 1009 매일경제 1015 한국경제 1014 파이낸셜 뉴스 1016 헤럴드경제

            engine = sqlalchemy.create_engine(f'mysql://root:{DB_SECRET}@localhost:3306/sqldb', encoding='utf8')
            conn = engine.connect()

            # get stock lists from the tradable DB Pool
            sql = f'select market , ticker , stock_name  from tb_predicted_stock_pool tpsp where use_yn ="y"'

            filtered_stock_list = pd.read_sql(sql, engine)

            len_filtered_stock_list = len(filtered_stock_list)
            start_num = 0
            # print("len_filtered_stock_list count : " , len_filtered_stock_list)

            # for loop by each stock
            for idx in range(start_num, len_filtered_stock_list):
                print('ticker : ', filtered_stock_list.iloc[idx]['ticker'], 'Market : ',
                      filtered_stock_list.iloc[idx]['market'], 'Name : ', filtered_stock_list.iloc[idx]['stock_name'])

                ticker = filtered_stock_list.iloc[idx]['ticker']
                stock_name = filtered_stock_list.iloc[idx]['stock_name']

                # for loop by news site
                for press in press_lists:

                    # methodology: crawling from "today -1" to "today -1 -200"
                    date = datetime.date.today() - datetime.timedelta(days=1)
                    target_date = date - datetime.timedelta(days=200)  # crawling term
                    while target_date != date:

                        date_str = date.strftime('%Y%m%d')

                        url = f'{base_url}search.naver?where=news&query={stock_name}&mynews=1&news_office_checked={press}&nso=so:dd,p:from{date_str}to{date_str}'
                        response = requests.get(url)

                        # no news return process
                        if response.status_code == 200:
                            html = response.text
                            soup = BeautifulSoup(html, 'html.parser')
                            if soup.find("div", "news_info") is None:
                                date -= datetime.timedelta(days=1)
                                continue
                        else:
                            # to leave log(temp_log.txt) in the case of abnormal response
                            print("response.status_code : ", response.status_code, url)
                            print("ignore", response.status_code)
                            time.sleep(5)
                            date -= datetime.timedelta(days=1)
                            continue

                        yield scrapy.Request(url=url, meta={'ticker': ticker, 'stock_name': stock_name},
                                             callback=self.parse_url, dont_filter=True)

                        # random use the user_agent (to avoid 403 return)
                        # yield scrapy.Request(url=url, meta={'ticker': ticker, 'stock_name': stock_name},
                        #                      callback=self.parse_url, dont_filter=True, headers={
                        #         "User-Agent": user_agent_list[random.randint(0, len(user_agent_list) - 1)]})

                        date -= datetime.timedelta(days=1)
        except Exception as e:
            conn.close()
            print("Exception : ", e)

    def parse_url(self, response):
        try:
            # request the href link in the page that requested above
            for news_data in response.css('.list_news li .news_area .news_tit::attr(href)').getall():
                url = news_data
                # error link if total href link is the same with below
                if "http://news.heraldcorp.com/" == url:
                    continue
                print("link here: ", response.url, url)
                yield scrapy.Request(url=url, callback=self.parse_news, meta=response.meta, dont_filter=True)

                # random use the user_agent (to avoid 403 return)
                # yield scrapy.Request(url=url, callback=self.parse_news, meta=response.meta, dont_filter=True, headers={
                #     "User-Agent": user_agent_list[random.randint(0, len(user_agent_list) - 1)]})

        except Exception as e:
            print(f'exception in parse_url : {e}')

    def parse_news(self, response):

        scrawl_info = NewsspiderItem()

        scrawl_info['stock_name'] = response.meta['stock_name'].strip()
        scrawl_info['ticker'] = response.meta['ticker']

        # each publisher using different naming rule
        content = ''.join(response.css('#articleText p::text').getall())

        if content == '':
            content = ''.join(response.css('#articleText::text').getall())
            if content == '':
                content = ''.join(response.css('.news_cnt_detail_wrap p::text').getall())
                if content == '':
                    content = ''.join(response.css('.article-view-content::text').getall())

        if content == '':
            content = ''.join(response.css('.article_content::text').getall())
            if content == '':
                content = ''.join(response.css('.post-content .description::text').getall())
                if content == '':
                    content = ''.join(response.css('.read_txt::text').getall())
                    if content == '':
                        content = ''.join(response.css('.news_cnt_detail_wrap::text').getall())
                        if content == '':
                            content = ''.join(response.css('.view_txt::text').getall())
                            if content == '':
                                content = ''.join(response.css('.art_txt::text').getall())
                                if content == '':
                                    content = ''.join(response.css('.cont_art::text').getall())
                                    if content == '':
                                        content = ''.join(response.css('.con_article::text').getall())
        try:
            scrawl_info['content'] = content.replace('\n', '').strip()
        except Exception as e:
            print(e)
            scrawl_info['content'] = content

        published_date = response.css('li.article_date::text').get()



        if published_date is None:
            published_date = response.css('.sm_num::text').get()
            if published_date is None:
                published_date = response.css('.lasttime::text').get()
                if published_date is None:
                    published_date = response.css('.txt_left::text').get()
                    if published_date is None:
                        published_date = response.css('.src_date2::text').get()
                        if published_date is None:
                            published_date = response.css('.view_img p::text').get()
                            if published_date is None:
                                published_date = response.css('.date span::text').get()
                                if published_date is None:
                                    published_date = response.css('.lasttime1::text').get()
                                    if published_date is None:
                                        published_date = response.css('.dtreviewed time::attr(title)').get()
                                        if published_date is None:
                                            published_date = response.css('.byline em::text')[1].get()
                                            if published_date is None:
                                                published_date = response.css('ul.infomation li::text')[2].get()
        try:
            scrawl_info['published_date'] = published_date.strip()
        except Exception as e:
            print("published_date error : ", response.url, e)
            scrawl_info['published_date'] = published_date
        scrawl_info['created_date'] = datetime.date.today()

        if scrawl_info['published_date'] is None or scrawl_info['content'] == '':
            raise Exception("No data in scrawl_info['published_date'] or scrawl_info['content']")

        yield scrawl_info
