# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class CrawlerItem(scrapy.Item):
    content = scrapy.Field()
    doc_type = scrapy.Field()
    start_url = scrapy.Field()
    unit_name = scrapy.Field()

    files = scrapy.Field()
    file_urls = scrapy.Field()
    file_titles = scrapy.Field()
    file_names = scrapy.Field()

