# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import time
import pymysql
import pymysql.cursors
import scrapy
from rca_crawler import settings
from rca_crawler.items import CrawlerItem
from scrapy.pipelines.files import FilesPipeline
#from org.itri.icl.rcm.rwc.formatter.WebsiteFormatter import WebsiteFormatter

class RenameFilesPipeline(FilesPipeline):

    # 覆寫預設儲存檔名方式
    def get_media_requests(self, item, info):
        if "file_urls" in item:
            for i, file_url in enumerate(item["file_urls"]):
                yield scrapy.Request(url=file_url, meta={"item": item, "index": i})

    # 覆寫預設儲存檔名方式
    def file_path(self, request, response=None, info=None):
        index = request.meta["index"]
        dir = request.meta["item"]["unit_name"]
        filename = request.meta["item"]["file_names"][index]
        # 檢查檔案長度
        if len(filename) > 100:
            index = len(filename) - 100
            filename = filename[index: ]

        filename = self.getUnrepeatedFilename(settings.FILES_STORE + os.sep + dir + os.sep, filename, 0)
        request.meta["item"]["file_names"][index] = filename
        return dir + os.sep + filename

    # 取得不重複的檔案名稱
    def getUnrepeatedFilename(self, dir, filename, index):
        f = filename
        if index > 0:
            fname, extension = os.path.splitext(filename)
            f = fname + "_" + str(index + 1) + extension

        # 避免過多的迴圈
        if os.path.exists(dir + f):
            if index < 10:
                f = self.getUnrepeatedFilename(dir, filename, index + 1)
            else:
                f = str(int(time.time())) + extension

        return f

class CrawlerPipeline(FilesPipeline):
    conn = None
    cursor = None

    def open_spider(self, spider):
        self.conn = pymysql.connect(host=settings.MYSQL_HOST,
             user=settings.MYSQL_USER,
             password=settings.MYSQL_PASSWORD,
             database=settings.MYSQL_DBNAME,
             charset='utf8',
             cursorclass=pymysql.cursors.DictCursor)
        
        self.cursor = self.conn.cursor()
    
    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        if isinstance(item, CrawlerItem):

            try:
                sql = """
                    INSERT INTO WebsiteDoc
                    (url, status, plainText, unitName, docType) VALUES
                    (%s, %s, %s, %s, %s)
                """
                inputs = [
                    item["start_url"], 0, item["content"], item["unit_name"], item["doc_type"]
                ]

                self.cursor.execute(sql, inputs)
                last_id = self.cursor.lastrowid
                #print("lastID =", last_id)

                # 新增檔案
                if "files" in item:
                    for index, f in enumerate(item["files"]):
                        sql = """
                            INSERT INTO WebsiteDocFile
                            (websiteDocID, title, name) VALUES
                            (%s, %s, %s)
                        """
                        inputs = [
                            last_id, item["file_titles"][index], item["file_names"][index]
                        ]
                        self.cursor.execute(sql, inputs)

                self.conn.commit()

            except Exception as e:
                self.conn.rollback()
                print(e)

        return item
