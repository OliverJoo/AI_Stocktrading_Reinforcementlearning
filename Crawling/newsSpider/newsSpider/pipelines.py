    # Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exporters import CsvItemExporter
import pymysql


# from scrapy.utils.project import get_project_settings
# settings = get_project_settings()

class NewsspiderPipeline:

    def __init__(self):
        self.connect = pymysql.connect(
            host='localhost',
            db='sqldb',
            user='root',
            passwd='0411',
            charset='utf8',
            use_unicode=True
        )
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        try:
            self.cursor.execute(
                "insert into tb_contents (ticker,stock_name,published_date,contents,created_date, use_yn, identity) value(%s,%s,%s,%s,now(),'y','news')",
                (item['ticker'],
                 item['stock_name'],
                 item['published_date'],
                 item['content']
                 ))
            self.connect.commit()

        except Exception as error:
            print(error)
        return item

    def close_spider(self, spider):
        self.connect.close()

