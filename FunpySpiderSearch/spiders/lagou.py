from scrapy import FormRequest, Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from datetime import datetime

import logging
import json
import uuid
import copy

from FunpySpiderSearch.sites.lagou.lagou_Item import LagouJobItem, LagouJobItemLoader
from FunpySpiderSearch.utils.common import get_md5


def get_uuid():
    return str(uuid.uuid4())


class LagouJobspider(CrawlSpider):
    max_start_request_num = 50

    search_city = '长沙'
    search_keyword = ['js', '全栈', 'golang', 'php', 'python']

    name = 'lagou'
    allowed_domains = ['www.lagou.com']
    agent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/" \
            "537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36"
    custom_settings = {
        "COOKIES_ENABLED": True,
        "COOKIES_DEBUG": False,
        "REFERER_ENABLED": False,
        "DOWNLOAD_DELAY": 5,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Host': 'www.lagou.com',
            'Origin': 'https://www.lagou.com',
            'Referer': 'https://www.lagou.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
        }
    }

    myCookies = {
        '_ga': 'GA1.2.1113203059.1544346151',
        '_gid': 'GA1.2.1792021447.1544346151',
        'Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6': '1544346152',
        'user_trace_token': '20181209170232-29baeb0e-fb91-11e8-8ebd-525400f775ce',
        'LGUID': '20181209170232-29baf0ed-fb91-11e8-8ebd-525400f775ce',
        'index_location_city': '%E6%B7%B1%E5%9C%B3',
        'TG-TRACK-CODE': 'search_code',
        'LGSID': '20181210203705-4d1fb6e1-fc78-11e8-8ced-5254005c3644',
        'PRE_UTM': '',
        'PRE_HOST': '',
        'PRE_SITE': '',
        'PRE_LAND': 'https%3A%2F%2Fwww.lagou.com%2F',
        'SEARCH_ID': '435b5a651ee14fde9e79b891dfc30488',
        'LGRID': '20181210203725-58ffef05-fc78-11e8-8f76-525400f775ce',
        'Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6': '1544445448',
    }

    def generate_cookie(self):
        token = get_uuid()
        cookie = copy.copy(self.myCookies)
        cookie['JSESSIONID'] = get_uuid()
        cookie['LGSID'] = get_uuid()
        cookie['SEARCH_ID'] = get_uuid()
        cookie['user_trace_token'] = token
        cookie['LGUID'] = token
        cookie['LGRID'] = token
        return cookie


    def start_requests(self):
        kds = self.search_keyword
        city = self.search_city
        headers = self.custom_settings['DEFAULT_REQUEST_HEADERS']
        headers['Referer'] = 'https://www.lagou.com/jobs/list_php%E5%90%8E%E7%AB%AF \
            ?oquery=PHP&fromSearch=true&labelWords=relative&city=%E9%95%BF%E6%B2%99'
        headers['X-Requested-With'] = 'XMLHttpRequest'
        headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'

        url = "https://www.lagou.com/jobs/positionAjax.json?city="+city+"&needAddtionalResult=false"

        req_urls = []
        
        for num in range(self.max_start_request_num):
            for kd in kds:
                isFirst = 'true' if  num == 0 else 'false'
                c = self.generate_cookie()
                req_urls.append(FormRequest(url=url,
                        formdata={'first': isFirst, 'pn': str(num+1), 'kd': kd},
                        callback=self.parse_ajaxjson,
                        cookies=c,
                        headers=headers))        
        return req_urls

    def parse_ajaxjson(self, response):
        json_data = response.body.decode(response.encoding)

        data = json.loads(json_data)
        if (data['success']) :
            for info in data['content']['positionResult']['result']:
                yield Request(url='https://www.lagou.com/jobs/%s.html' % (info['positionId']), 
                    callback=self.parse_content,
                    cookies=self.generate_cookie())
        else:
            logging.error("获取错误：" + data['msg'] + "\n" + response.request.body.decode(response.request.encoding))
        
    @staticmethod
    def parse_content(response):
        item_loader = LagouJobItemLoader(item=LagouJobItem(), response=response)
        item_loader.add_css("title", ".job-name::attr(title)")
        item_loader.add_value("url", response.url)
        item_loader.add_value("url_object_id", get_md5(response.url))
        item_loader.add_css("salary_min", ".job_request .salary::text")
        item_loader.add_xpath("job_city", "//*[@class='job_request']/p/span[2]/text()")
        item_loader.add_xpath("work_years_min", "//*[@class='job_request']/p/span[3]/text()")
        item_loader.add_xpath("degree_need", "//*[@class='job_request']/p/span[4]/text()")
        item_loader.add_xpath("job_type", "//*[@class='job_request']/p/span[5]/text()")
        item_loader.add_css("tags", '.position-label li::text')
        item_loader.add_css("publish_time", ".publish_time::text")
        item_loader.add_css("job_advantage", ".job-advantage p::text")
        item_loader.add_css("job_desc", ".job_bt div")
        item_loader.add_css("job_addr", ".work_addr")
        item_loader.add_css("company_name", "#job_company dt a img::attr(alt)")
        item_loader.add_css("company_url", "#job_company dt a::attr(href)")
        item_loader.add_value("crawl_time", datetime.now())

        job_item = item_loader.load_item()

        return job_item
