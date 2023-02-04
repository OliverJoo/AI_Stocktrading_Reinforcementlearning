# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsspiderItem(scrapy.Item):
    # define the fields for your item here like:
    stock_name = scrapy.Field()
    ticker = scrapy.Field()
    content = scrapy.Field()
    published_date = scrapy.Field()
    created_date = scrapy.Field()

    pass
