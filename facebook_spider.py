import pymysql
from lxml.html import fromstring
import scrapy
import re
import pandas
from FacebookParseObject import FacebookObjectSummarize, return_region
from http.cookies import SimpleCookie
from elasticsearch import Elasticsearch
from scrapy.downloadermiddlewares.retry import RetryMiddleware


class FacebookDownloaderMiddleware(RetryMiddleware):

    def process_response(self, request, response, spider):
        if "It looks like you were misusing this feature by going too fast. " in response.text or r"s\u1eed d\u1ee5ng " \
                                                                                                  r"qu\u00e1 nhanh" \
                in response.text :
            with open("bad_cookies.txt", "a", encoding="utf-8") as bad_cookies_file:
                bad_cookies_file.write(f'Cookies {request.cookies.get("c_user")} is unusable. REASON: OVER HEAT ! \n')
            return response
        if "/checkpoint" in response.url or "about?checkpoint_src=" in response.url:
            with open("bad_cookies.txt", "a", encoding="utf-8") as bad_cookies_file:
                bad_cookies_file.write(f'Cookies {request.cookies.get("c_user")} is unusable. REASON: DEAD COOKIE ! \n' )
            return response
        if "login" in response.url:
            with open("bad_cookies.txt", "a", encoding="utf-8") as bad_cookies_file:
                bad_cookies_file.write(f'Cookies {request.cookies.get("c_user")} is unusable. REASON: BAD COOKIE ! \n' )
            return response
        with open("bad_cookies.txt", "r", encoding="utf-8") as bad_cookies_file:
            uniqlines = set(bad_cookies_file.readlines())
        with open("bad_cookies.txt", "w", encoding="utf-8") as bad_cookies_file:
            bad_cookies_file.writelines(list(uniqlines))
        return response

def get_cookies():
    df = pandas.read_csv("acc_profile.txt", sep="|")
    return df["cla"].tolist()


class FacebookSpiderPipeline:
    def __init__(self):
        uris = "https://elastic:btCyo9WolIzSYS1hLwdk@10.30.3.45:9200"
        self.es = Elasticsearch(uris, verify_certs=False)

    def process_item(self, item, spider):
        self.es.index(index="facebook-information-data", body=dict(item))
        return item


class FacebookSpiderItem(scrapy.Item):
    uid = scrapy.Field()
    gender = scrapy.Field()
    location = scrapy.Field()
    region = scrapy.Field()
    age = scrapy.Field()
    central = scrapy.Field()
    number_phone = scrapy.Field()
    pass


class FacebookSpider(scrapy.Spider):
    df = pandas.read_csv("Provinces.csv")
    list_cookies = []

    def __init__(self, cookies_string, *args, **kwargs):
        self.headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'viewport-width': '1680',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
                          ' (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'empty',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,'
                      'image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'sec-ch-ua-mobile': '?0',
            'Cookie': f'{cookies_string}'
        }

        cookie = SimpleCookie()
        cookie.load(cookies_string.strip())
        cookies = {}
        for key, morsel in cookie.items():
            cookies[key] = morsel.value
        self.cookies = cookies
        super(FacebookSpider).__init__(*args, **kwargs)

    # for coolie in get_cookies()
    name = "Facebook spider"
    def start_requests(self):
        urls = []
        phones = []
        db = pymysql.connect(host="10.30.3.48", user="vnphone", passwd="Intlabs@2021", db="vnphone", port=3306,
                             charset='utf8', autocommit=True)
        cursor = db.cursor()
        cursor.execute(f"DELETE FROM vnphone LIMIT 500")
        cursor.execute(f"SELECT * FROM vnphone LIMIT 500")
        results = cursor.fetchall()
        uid = [result[2] for result in results]
        phone = [result[1] for result in results]
        for each_uid, each_phone in zip(uid, phone):
            param_about = "/about"
            url = "https://www.facebook.com/" + each_uid + param_about
            urls.append(url)
            phones.append(each_phone)

        return [scrapy.http.Request(url=url, callback=self.parse,
                                    meta={'dont_redirect': False, 'handle_httpstatus_list': [500], "phone": phone, "uid":url.replace("https://www.facebook.com/", "").replace("/about", "")}
                                    , headers=self.headers, cookies=self.cookies, dont_filter=False) for url, phone in
                zip(urls, phones)]

    def parse(self, response, **kwargs):
        items_facebook = FacebookSpiderItem()
        user_id = response.meta.get("uid")
        tree = fromstring(response.text)
        if response.status == 500:
            return scrapy.http.Request(url=response.url.replace("/about", ""), callback=self.parse
                                       , headers=self.headers, cookies=self.cookies, dont_filter=False)
        object_facebook = FacebookObjectSummarize(tree)
        object_facebook.object_sum()
        # print(self.provinces, self.districts)
        items_facebook["uid"] = user_id
        items_facebook["number_phone"] = response.meta.get("phone")
        if object_facebook.gender == "MALE":
            object_facebook.gender = 101
        elif object_facebook.gender == "FEMALE":
            object_facebook.gender = 102
        elif not isinstance(object_facebook.gender, int):
            object_facebook.gender = 103
        items_facebook["gender"] = object_facebook.gender
        items_facebook["central"] = 103
        items_facebook["region"] = "Việt Nam"
        items_facebook["location"] = object_facebook.location
        location = object_facebook.location
        try:
            if location is not None:
                location = location.replace("Lives in ", "") \
                    .replace("Sống tại ", "") \
                    .replace(" City", "") \
                    .replace("Hanoi", "Ha Noi") \
                    .replace("Thành phố ", "") \
                    .replace(" (thành phố)", "") \
                    .replace(" (thị xã)", "") \
                    .strip()
                items_facebook["location"] = location
                items_facebook["region"], items_facebook["central"], u = return_region(
                    each_loc=location, df=self.df)
        except Exception as e:
            items_facebook["central"] = 103
            items_facebook["location"] = location
            items_facebook["region"] = "Việt Nam"
        items_facebook["age"] = None
        if f'username_for_profile":"","id":"{user_id}' in response.text:
            try:
                link = tree.xpath("//*[contains(text(), '/about_contact_and_basic_info')]//text()")
                link = re.findall(r'(?="url":")(.*)(?=\\/about_contact_and_basic_info)', link[0])[0].split("url")[
                    -1].replace('":"', "").replace("\\", "")
                link = link + "/about_contact_and_basic_info"
            except IndexError as e:
                link = "https://www.facebook.com/profile.php?id=" + user_id + "&sk=about_contact_and_basic_info"
            return scrapy.http.Request(url=link, callback=self.parse_age, meta={"items": items_facebook}
                                       , headers=self.headers, cookies=self.cookies)
        else:

            return items_facebook

    def parse_age(self, response, **kwargs):
        object_facebook = FacebookObjectSummarize(fromstring(response.text))
        items_facebook = response.meta.get("items")
        items_facebook["age"] = object_facebook.get_profile_age()
        yield items_facebook


import scrapy
from scrapy.crawler import CrawlerProcess
from twisted.internet.task import deferLater


def sleep(self, *args, seconds):
    """Non blocking sleep callback"""
    return deferLater(reactor, seconds, lambda: None)


process = CrawlerProcess(
    settings={"CONCURRENT_REQUESTS": 1,
              "RETRY_TIMES": 2,
              "DOWNLOADER_MIDDLEWARES": {
                  '__main__.FacebookDownloaderMiddleware': 543,
              },
              "ITEM_PIPELINES": {
                  '__main__.FacebookSpiderPipeline': 300,
              },
              "DOWNLOAD_DELAY": 0,
              }
)
list_cookies = get_cookies()
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging


class Run_Spider_From_SubClass:

    def __init__(self, list_cookies, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.list_cookies = list_cookies

        configure_logging()
        self.runner = CrawlerRunner({
                                     "CONCURRENT_REQUESTS": 256,
                                     "CONCURRENT_REQUESTS_PER_DOMAIN": 256,
                                     "CONCURRENT_REQUESTS_PER_IP": 256,
                                     "REACTOR_THREADPOOL_MAXSIZE" :128,
                                     "AUTOTHROTTLE_TARGET_CONCURRENCY" : 128,
                                     "RETRY_TIMES": 2,
                                     "DOWNLOADER_MIDDLEWARES": {
                                         '__main__.FacebookDownloaderMiddleware': 543,
                                     },
                                     "ITEM_PIPELINES": {
                                         '__main__.FacebookSpiderPipeline': 300,
                                     },
                                     "DOWNLOAD_DELAY": 0,
                                     })

    @defer.inlineCallbacks
    def crawl(self):
        x = 0
        while x < 100:
            for cookies in self.list_cookies:
                yield self.runner.crawl(FacebookSpider, cookies_string=cookies)
            x = x + 1
        reactor.stop()

    def run_spider_in_loop(self):
        self.crawl()
        reactor.run()


list_cookies = list_cookies
runner = Run_Spider_From_SubClass(list_cookies)
runner.run_spider_in_loop()
