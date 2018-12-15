from scrapy import FormRequest, Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from datetime import datetime

import logging
import json
import uuid
import copy
import urllib

from FunpySpiderSearch.sites.zhilian.zhilian_Item import ZhilianItem, ZhilianItemLoader
from FunpySpiderSearch.utils.common import get_md5


HOT_CITY_MAP = {"全国": "489", "北京": "530", "上海": "538", "深圳": "765", "广州": "763", "天津": "531", "成都": "801", "杭州": "653", "武汉": "736", "大连": "600", "长春": "613", "南京": "635", "济南": "702", "青岛": "703", "苏州": "639", "沈阳": "599", "西安": "854", "郑州": "719", "长沙": "749", "重庆": "551", "哈尔滨": "622", "无锡": "636", "宁波": "654", "福州": "681", "厦门": "682", "石家庄": "565", "合肥": "664", "惠州": "773"}

class Zhilianspider(CrawlSpider):
    max_start_request_num = 50

    search_city = '长沙'
    search_keyword = ['js', '全栈', 'golang', 'php', 'python', '前端']
    # search_keyword = ['js']

    name = 'zhilian'
    allowed_domains = ['fe-api.zhaopin.com', 'jobs.zhaopin.com']
    agent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/" \
            "537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36"
    custom_settings = {
        "COOKIES_ENABLED": False,
        "COOKIES_DEBUG": False,
        "REFERER_ENABLED": False,
        "DOWNLOAD_DELAY": 1,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Origin': 'https://sou.zhaopin.com',
            'Referer': 'https://sou.zhaopin.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
        }
    }

    def start_requests(self):
        kds = self.search_keyword
        city = HOT_CITY_MAP[self.search_city]
        headers = self.custom_settings['DEFAULT_REQUEST_HEADERS']
        page_size = 90
        url = "https://fe-api.zhaopin.com/c/i/sou"
        query = "?start=%s&cityId=%s&kw=%s"
        query2 = "&workExperience=-1&education=-1&pageSize=90&companyType=-1&employmentType=-1&jobWelfareTag=-1&kt=3&_v=0.56031511&x-zp-page-request-id=66c998d928ca4f55921006c724388fe6-1544785160796-668022"

        req_urls = []
        
        for num in range(self.max_start_request_num):
            for kd in kds:
                rurl = url + (query % (num * page_size, city, kd)) + query2
                req_urls.append(Request(url=rurl,
                    headers=headers,
                    callback=self.parse_ajaxjson))
        return req_urls

    def parse_ajaxjson(self, response):
        json_data = response.body.decode(response.encoding)

        data = json.loads(json_data)
        if data['code'] == 200 :
            for info in data['data']['results']:
                yield Request(url=info['positionURL'],
                    callback=self.parse_content)
        else:
            logging.error("获取错误：" + data['msg'] + "\n" + response.request.body.decode(response.request.encoding))

    @staticmethod
    def parse_content(response):
        item_loader = ZhilianItemLoader(item=ZhilianItem(), response=response)
        item_loader.add_css("title", "div.main1-stat h1.info-h3::text")
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_css("salary_min", "div.main1-stat div.info-money>strong::text")
        item_loader.add_css("job_city", "div.main1-stat .info-three>span:nth-child(1)>a::text")
        item_loader.add_css("work_years_min", "div.main1-stat .info-three>span:nth-child(2)::text")
        item_loader.add_css("degree_need", "div.main1-stat .info-three>span:nth-child(3)::text")
        item_loader.add_xpath("job_advantage", "//script[1]")
        item_loader.add_css("job_desc", "div.responsibility > div.pos-ul")
        item_loader.add_css("job_addr", "div.work-add > p.add-txt::text")
        item_loader.add_css("company_name", "div.company > a::text")
        item_loader.add_css("company_url", "div.company > a::attr(href)")
        item_loader.add_value("crawl_time", datetime.now())

        job_item = item_loader.load_item()

        return job_item
