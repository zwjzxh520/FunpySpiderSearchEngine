# 招聘网站数据抓取
forked from https://github.com/mtianyan/FunpySpiderSearchEngine

环境相关信息，请参考原项目。我使用 pipenv 来管理环境。

抓取的数据会自动去重

去掉了写入 ElasticSearch 的步骤

薪水的单位都是 *k/月*

工作经验说明：
* work_years_min: 最低要求
* work_years_max: 最高要求

1. 都为 0，则表示没有特殊要求
2. 都为 0.5，表示应届生
3. 都为 999，没有抓到值


## 拉勾网招聘信息爬虫
原来的项目，只能从拉勾首页开始爬。稍做修改，支持爬取指定关键字和城市的招聘信息。
命令：
```bash
scrapy crawl lagou
```
##  51job网招聘信息爬虫
命令：
```bash
scrapy crawl w51job
```
python 中 import 路径不能以数字开头，因此加了个字母 w

与拉勾的数据相比，数据表中去掉了 *tags* 和 *job_type* 字段

##  智联招聘网招聘信息爬虫
命令：
```bash
scrapy crawl zhilian
```

与拉勾的数据相比，数据表中去掉了 *publish_time*、 *tags* 和 *job_type* 字段