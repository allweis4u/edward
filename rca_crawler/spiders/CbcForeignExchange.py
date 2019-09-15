
import scrapy
import re
import pymysql
import pymysql.cursors
from scrapy import Request
from rca_crawler import settings
from rca_crawler.items import CrawlerItem

class CbcForeignExchange(scrapy.Spider):
    name = "cbc_foreign_exchange"

    conn = None
    cursor = None
    urls_seen = set()
    page = 1
    start_url = "https://www.cbc.gov.tw/lp.asp?ctNode=379&CtUnit=125&BaseDSD=7&mp=1"

    def __init__(self):
        self.conn = pymysql.connect(host=settings.MYSQL_HOST,
             user=settings.MYSQL_USER,
             password=settings.MYSQL_PASSWORD,
             database=settings.MYSQL_DBNAME,
             charset='utf8',
             cursorclass=pymysql.cursors.DictCursor)
        
        self.cursor = self.conn.cursor()
        
        # 搜尋已有的url
        sql = """
            SELECT url FROM WebsiteDoc
        """
        self.cursor.execute(sql)
        datas = self.cursor.fetchall()
        for d in datas:
            self.urls_seen.add(d["url"])

    def __del__(self):
        self.conn.close()

    def start_requests(self):
        yield scrapy.FormRequest(url=self.start_url,
            callback=self.parse_list)

    def parse_list(self, response):
        linkObjs = response.css('.list li')
        count, limit = 0, len(linkObjs)
        for res in linkObjs:
            href = res.css('a::attr("href")').get()
            url = response.urljoin(href)

            if url in self.urls_seen:
                count += 1
            else:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_detail
                )

        if count < limit and self.page < 1:
            self.page += 1

            url = self.start_url + "&nowPage=" + str(self.page) + "&pagesize=15"
            yield scrapy.FormRequest(url=url,
                callback=self.parse_list)

    # 解析內頁
    def parse_detail(self, response):
        main = response.css('.Article')
        item = CrawlerItem()
        item["start_url"] = response.request.url
        item["content"] = main.get()
        item["doc_type"] = "mixExecute"
        item["unit_name"] = "CBC"
        item["file_urls"], item["file_titles"], item["file_names"] = [], [], []

        for linkObj in main.css(".download li"):
            file_url = response.urljoin(linkObj.css("a::attr('href')").get())
            title = linkObj.css("::text").get()
            filename = title

            # 找出檔名
            matches = re.findall("public/Attachment/(.+)", file_url)
            if len(matches) == 0:
                continue
            filename = str(matches[0])

            item["file_urls"].append(file_url)
            item["file_titles"].append(title)
            item["file_names"].append(filename)

        # 把資料交給pipeline處理
        yield item

