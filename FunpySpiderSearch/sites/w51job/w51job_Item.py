__author__ = 'Neil Zeng'
# 51job 不能 import， 因为以数字开头了，所以加了个 w

import datetime
import re
import scrapy
from elasticsearch_dsl import connections
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join
from w3lib.html import remove_tags
from FunpySpiderSearch.items import MysqlItem, ElasticSearchItem
from FunpySpiderSearch.settings import SQL_DATETIME_FORMAT
from FunpySpiderSearch.utils.common import real_time_count
from FunpySpiderSearch.utils.es_utils import generate_suggests
from FunpySpiderSearch.utils.mysql_utils import fun_sql_insert

JOB_COUNT_INIT = 0

JOB_DEGREE = ['高中', '中专', '大专', '本科', '研究生', '硕士', '博士', '博士后']

class W51JobItemLoader(ItemLoader):
    default_output_processor = TakeFirst()

# 将空白字符替换为 ,
def replace_space(src):
    return re.sub('[\t\n\r]+', ',', src.strip())

def get_city(str):
    return str.strip().split('-')[0]

class W51JobItem(scrapy.Item, MysqlItem, ElasticSearchItem):
    title = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    salary_min = scrapy.Field()
    salary_max = scrapy.Field()
    job_city = scrapy.Field(
        input_processor=Join('|'),
    )
    work_years_min = scrapy.Field()
    work_years_max = scrapy.Field()
    degree_need = scrapy.Field( )
    publish_time = scrapy.Field()

    job_advantage = scrapy.Field(
        input_processor=Join(','),
    )
    job_desc = scrapy.Field()
    job_addr = scrapy.Field(
        input_processor=Join(''),
    )
    company_name = scrapy.Field()
    company_url = scrapy.Field()
    tags = scrapy.Field()
    crawl_time = scrapy.Field()
    crawl_update_time = scrapy.Field()

    def clean_data(self):
        self["tags"] = ""

        misc_info = self['job_city'].strip().split('|')

        self['job_city'] = get_city(misc_info[0].strip())
        self['work_years_min'] = misc_info[1].strip()
        self['degree_need'] = misc_info[2].strip()

        if self['degree_need'] not in JOB_DEGREE:
            if self['degree_need'] == '初中及以下':
                self['degree_need'] = '初中'
            else:
                self['degree_need'] = ''

        if len(misc_info) > 4 and "发布" in misc_info[4]:
            self["publish_time"] = misc_info[4].strip()
        elif "发布" in misc_info[3]:
            self["publish_time"] = misc_info[3].strip()
        else:
            self['publish_time'] = ''

        try:
            self['job_advantage'] = self['job_advantage'].strip()
            if self['job_advantage']:
                self['job_advantage'] = replace_space(self['job_advantage'])
            else:
                self['job_advantage'] = ''
        except BaseException:
            self['job_advantage'] = ''


        self['job_addr'] = self['job_addr'].strip() if 'job_addr' in self else ''

        match_obj1 = re.match("(\d+)-(\d+)年经验", self['work_years_min'])
        match_obj2 = re.match("应届毕业生|无工作经验", self['work_years_min'])
        match_obj3 = re.match("经验不限", self['work_years_min'])
        match_obj4 = re.match("(\d+)年经验", self['work_years_min'])

        if match_obj1:
            self['work_years_min'] = match_obj1.group(1)
            self['work_years_max'] = match_obj1.group(2)
        elif match_obj2:
            self['work_years_min'] = 0.5
            self['work_years_max'] = 0.5
        elif match_obj3:
            self['work_years_min'] = 0
            self['work_years_max'] = 0
        elif match_obj4:
            self['work_years_min'] = match_obj4.group(1)
            self['work_years_max'] = match_obj4.group(1)
        else:
            self['work_years_min'] = 999
            self['work_years_max'] = 999

        match_salary = re.match("([\d\.]+)-([\d\.]+)([千万])/月", self['salary_min'] if 'salary_min' in self else '')
        if match_salary:
            # 如果是 万，则要 * 10
            wan_salary = 1 if (match_salary.group(2) == "千") else 10
            self['salary_min'] = float(match_salary.group(1)) * wan_salary
            self['salary_max'] = float(match_salary.group(2)) * wan_salary
        else:
            self['salary_min'] = 666
            self['salary_max'] = 666

        match_time3 = re.match("(\d+)-(\d+)发布", self["publish_time"])
        if match_time3:
            year = datetime.datetime.now().year
            month = int(match_time3.group(1))
            day = int(match_time3.group(2))
            today = datetime.datetime(year, month, day)
            self["publish_time"] = today.strftime(SQL_DATETIME_FORMAT)
        else:
            self["publish_time"] = datetime.datetime.now(
            ).strftime(SQL_DATETIME_FORMAT)
        self["crawl_time"] = self["crawl_time"].strftime(SQL_DATETIME_FORMAT)

    def save_to_mysql(self):
        self.clean_data()
        insert_sql = """
                    insert into 51job_job(title, url, url_object_id, salary_min, salary_max, job_city, work_years_min, work_years_max, degree_need,
                    publish_time, job_advantage, job_desc, job_addr, company_name, company_url, crawl_time) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE salary_min=VALUES(salary_min), salary_max=VALUES(salary_max), job_desc=VALUES(job_desc)
                """
        sql_params = (
            self["title"],
            self["url"],
            self["url_object_id"],
            self["salary_min"],
            self["salary_max"],
            self["job_city"],
            self["work_years_min"],
            self["work_years_max"],
            self["degree_need"],
            self["publish_time"],
            self["job_advantage"],
            self["job_desc"],
            self["job_addr"],
            self["company_name"],
            self["company_url"],
            self["crawl_time"]
        )

        return insert_sql, sql_params

    def save_to_es(self):
        # 不用写入 ES
        return

    def help_fields(self):
        for field in self.field_list:
            print(field, "= scrapy.Field()")


if __name__ == '__main__':
    W51JobItem().help_fields()
    instance = W51JobItem()
    sql, params = fun_sql_insert(field_list=instance.field_list, duplicate_key_update=instance.duplicate_key_update,
                                 table_name=instance.table_name)
