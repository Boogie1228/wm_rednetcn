import datetime
import hashlib
import json
import os
import random
import re
import time
from collections import deque
from urllib import parse
from lxml import etree
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger
# from proxy_list import hk_proxy_list
from wm_boxunus.proxy_list import hk_proxy_list

logger.add(
    "".join([__file__.split(".")[0], ".log"]),
    level='INFO',
    format='{time:YYYY-MM-DD HH:mm:ss} {level} {file}[line:{line}] {message}',
    rotation="100 MB",
    retention='10 days',
    encoding="utf-8"
)


class CrawlBoxun:
    def __init__(self):
        print("wm_youkucn >> __init__")
        self.DtpArticle_NAS_PATH = '/nas/wisecore/WiseConversion/converted_c4'
        self.topic = os.getenv("OUT_TOPIC", 'app.parser.content')
        # self.producer = KafkaProducer(
        #     bootstrap_servers=os.getenv("TO_KAFKA_HOSTS", '10.19.255.238:9092,10.19.255.138:9092,10.19.255.142:9092'),
        #     retries=3)1
        self.headers= {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
        self.duplicated = deque(maxlen=5000)

    def gen_file_path(self, url: str, pubcode):
        HHmm = (datetime.datetime.now()).strftime('%H%M')[-4:]
        filename = "DZ_" + HHmm + "_" + hashlib.md5(url.encode('utf-8')).hexdigest() + ".txt"
        yyyyMMdd = (datetime.datetime.now()).strftime('%Y%m%d')
        file_path = self.DtpArticle_NAS_PATH + "/" + yyyyMMdd + "/" + HHmm + "/" + pubcode + "/" + yyyyMMdd + "/" + filename
        return filename, file_path

    def send_to_kafka(self, data):
        print(data)
        # producer_res = self.producer.send(self.topic, json.dumps(data).encode('utf-8'))
        # producer_res.get(timeout=10)
        # producer_res.add_callback(self.kafka_success, data).add_errback(self.kafka_failure, data)
        # logger.info(f'send to kafka:{json.dumps(data, ensure_ascii=False)}')

    def kafka_success(self, item, f):
        url_ = item.get('urlList')
        print("send to kafka success! topic[%s] partition[%s] offset[%s] success! url : %s" % (
            f.topic, f.partition, f.offset, url_))

    def kafka_failure(self, item, f):
        url_ = item.get('urlList')
        print("send to kafka failure! url : %s. because: %s" % (url_, f.args[0]))

    def process_data(self, url, headline, section, timestamp, author, content, pub_code,listing_url=""):
        filename, file_path = self.gen_file_path(url, pub_code)
        self.send_to_kafka(
            {
                "pubTime": time.strftime("%H:%M:%S", time.localtime(timestamp)),
                "pubcode": pub_code,
                "pubdate": time.strftime("%Y%m%d", time.localtime(timestamp)),
                "section": section,
                "headline": headline,
                "author": author,
                "crawler_id": 12,
                "cts": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())),
                "filename": filename,
                "forward": "-1",
                "likeCount": "-1",
                "urlList": [
                    parse.unquote(url) + ";W;1"
                ],
                "viewCount": "-1",
                "wcImageUrlList": [],
                "filePath": file_path,
                "extJson": "{\"listingUrl\": \"" + f"{listing_url}" + "\"}",
                # "cutoffPubdate": "20200101",
                "content": content
            }
        )
    #
    def get_proxy(self):
        proxy = random.choice(hk_proxy_list)
        return {
            'http': 'http://' + proxy,
            'https': 'https://' + proxy
        }

    def download_page(self, url,data=None,method="GET",):
        try:
            response = requests.request(method,url=url, data= data, headers=self.headers,timeout=10)
            return response
        except:
            response = self.download_page(self, url,data=data,method="GET",)
            return response





    def parser_content(self, content):
        try:
            tree = etree.HTML(content)
            content = "".join(tree.xpath(".//text()"))
        except:
            pass
        content = content.replace("<div>","").replace("</div>","").replace("</p>","\n").replace("<p>","")
        content = content.replace("&ldquo;","").replace("&rdquo;","").replace("</a>","")
        content = content.replace("<span>","").replace("</span>","")
        content =  re.sub("<a.*?>","",content)
        return content



    def start(self):
        logger.info('start')
        try:  #
            listing_urls = os.getenv('listing_urls',
                                     'https://people.rednet.cn/#/leaveMsgList?typeid=1,https://people.rednet.cn/#/leaveMsgList?typeid=8,https://people.rednet.cn/#/leaveMsgList?typeid=2,https://people.rednet.cn/#/leaveMsgList?typeid=10,https://people.rednet.cn/#/leaveMsgList?isreply=1,https://people.rednet.cn/#/columnList?id=11,https://people.rednet.cn/#/leaveMsgList?cityid=434000,https://people.rednet.cn/#/leaveMsgList?cityid=430100,https://people.rednet.cn/#/leaveMsgList?cityid=430200,https://people.rednet.cn/#/leaveMsgList?cityid=430300,https://people.rednet.cn/#/leaveMsgList?cityid=430500,https://people.rednet.cn/#/leaveMsgList?cityid=430600,https://people.rednet.cn/#/leaveMsgList?cityid=430700,https://people.rednet.cn/#/leaveMsgList?cityid=430800,https://people.rednet.cn/#/leaveMsgList?cityid=430900,https://people.rednet.cn/#/leaveMsgList?cityid=431000,https://people.rednet.cn/#/leaveMsgList?cityid=431100,https://people.rednet.cn/#/leaveMsgList?cityid=431200,https://people.rednet.cn/#/leaveMsgList?cityid=431300,https://people.rednet.cn/#/leaveMsgList?cityid=433100,https://wz.rednet.cn/#/column?id=11,https://wz.rednet.cn/#/column?id=8,https://wz.rednet.cn/#/column?id=12,https://wz.rednet.cn/#/leaveMsgList?reply=1,https://wz.rednet.cn/#/column?id=5,https://wz.rednet.cn/#/leaveMsgList?reply=,https://wz.rednet.cn/#/column?id=6,https://wz.rednet.cn/#/column?id=4,https://people.rednet.cn/#/columnList?id=42,https://people.rednet.cn/#/columnList?id=14').split(
                ',')

            pubcode = 'wm_rednetcn'
            n = 0
            for listing_url in listing_urls:
                if 'typeid' in listing_url:
                    section = '百姓呼声'
                    REQ_url = "https://rapi.rednet.cn/front/msg/index"
                    type_id = re.findall("typeid=(\d+)", listing_url)[0]
                    data = '{"is_reply":"1","page":1,"page_num":20,"type_id":"%s","cate_id":"","cate_child_id":"","city_id":"","area_id":"","is_video":"","start_time":"","end_time":"","search":"","currentPage":1,"pageSize":20}' % str(
                        type_id)
                    Main_json = self.download_page(REQ_url, data, method="POST").json()["data"]["rows"]
                    for Per_json in Main_json:
                        content_url = "https://rapi.rednet.cn/front/msg/detail?id=" + str(Per_json["id"])
                        timeArray = time.strptime(Per_json["created_at"], "%Y-%m-%d %H:%M:%S")
                        timestamp = int(time.mktime(timeArray))
                        headline = Per_json["title"]
                        author = Per_json["nickname"]
                        content = self.download_page(content_url, method="GET").json()["data"]["content"]
                        content = self.parser_content(content)
                        content_url = "https://people.rednet.cn/#/leaveMsgDetails?id=" + str(Per_json["id"])
                        self.process_data(content_url, headline, section, timestamp, author, content, pubcode,
                                          listing_url)

                if 'isreply' in listing_url:
                    section = '百姓呼声'
                    REQ_url = "https://rapi.rednet.cn/front/msg/index"
                    type_id = re.findall("isreply=(\d+)", listing_url)[0]
                    data = '{"is_reply":"%s","page":1,"page_num":20,"type_id":"","cate_id":"","cate_child_id":"","city_id":"","area_id":"","is_video":"","start_time":"","end_time":"","search":"","currentPage":1,"pageSize":20}' % str(
                        type_id)
                    Main_json = self.download_page(REQ_url, data, method="POST").json()["data"]["rows"]
                    for Per_json in Main_json:
                        content_url = "https://rapi.rednet.cn/front/msg/detail?id=" + str(Per_json["id"])
                        timeArray = time.strptime(Per_json["created_at"], "%Y-%m-%d %H:%M:%S")
                        timestamp = int(time.mktime(timeArray))
                        headline = Per_json["title"]
                        author = Per_json["nickname"]
                        content = self.download_page(content_url, method="GET").json()["data"]["content"]
                        content = self.parser_content(content)
                        content_url = "https://people.rednet.cn/#/leaveMsgDetails?id=" + str(Per_json["id"])
                        self.process_data(content_url, headline, section, timestamp, author, content, pubcode,
                                          listing_url)

                if 'cityid' in listing_url:
                    section = '百姓呼声'
                    REQ_url = "https://rapi.rednet.cn/front/msg/index"
                    cityid = re.findall("cityid=(\d+)", listing_url)[0]
                    data = '{"is_reply":"3","page":1,"page_num":20,"type_id":"","cate_id":"","cate_child_id":"","city_id":"%s","area_id":"","is_video":"","start_time":"","end_time":"","search":"","currentPage":1,"pageSize":20}' % str(
                        cityid)
                    Main_json = self.download_page(REQ_url, data, method="POST").json()["data"]["rows"]
                    for Per_json in Main_json:
                        content_url = "https://rapi.rednet.cn/front/msg/detail?id=" + str(Per_json["id"])
                        timeArray = time.strptime(Per_json["created_at"], "%Y-%m-%d %H:%M:%S")
                        timestamp = int(time.mktime(timeArray))
                        headline = Per_json["title"]
                        author = Per_json["nickname"]
                        content = self.download_page(content_url, method="GET").json()["data"]["content"]
                        content = self.parser_content(content)
                        content_url = "https://people.rednet.cn/#/leaveMsgDetails?id=" + str(Per_json["id"])
                        self.process_data(content_url, headline, section, timestamp, author, content, pubcode,
                                          listing_url)

                if 'columnList?id=' in listing_url:
                    section = '百姓呼声'
                    REQ_url = "https://rapi.rednet.cn/front/article/index"
                    id = re.findall("columnList\?id=(\d+)", listing_url)[0]
                    data = '{"search":"","id":"%s","currentPage":1,"pageSize":10,"page_num":10,"page":1}' % str(id)
                    Main_json = self.download_page(REQ_url, data, method="POST").json()["data"]["rows"]
                    for Per_json in Main_json:
                        content_url = "https://rapi.rednet.cn/front/msg/detail?id=" + str(Per_json["id"])
                        timeArray = time.strptime(Per_json["published_at"], "%Y-%m-%d %H:%M:%S")
                        timestamp = int(time.mktime(timeArray))
                        headline = Per_json["title"]
                        author = None
                        content = self.download_page(content_url, method="GET").json()["data"]["content"]
                        content = self.parser_content(content)
                        content_url = "https://people.rednet.cn/#/leaveMsgDetails?id=" + str(Per_json["id"])
                        self.process_data(content_url, headline, section, timestamp, author, content, pubcode,
                                          listing_url)

                if 'wz.rednet.cn/#/column' in listing_url:
                    section = '问政湖南'
                    REQ_url = "https://wzapi.rednet.cn/front/article/index"
                    id = re.findall("column\?id=(\d+)", listing_url)[0]
                    data = '{"id":"%s","search":"","page":1}' % str(id)
                    Main_json = self.download_page(REQ_url, data, method="POST").json()["data"]["rows"]
                    for Per_json in Main_json:
                        content_url = "https://wzapi.rednet.cn/front/article/detail?id=" + str(Per_json["id"])
                        timeArray = time.strptime(Per_json["published_at"], "%Y-%m-%d %H:%M:%S")
                        timestamp = int(time.mktime(timeArray))
                        headline = Per_json["title"]
                        author = None
                        content = self.download_page(content_url, method="GET").json()["data"]["content"]
                        content = self.parser_content(content)
                        content_url = "https://people.rednet.cn/#/leaveMsgDetails?id=" + str(Per_json["id"])
                        self.process_data(content_url, headline, section, timestamp, author, content, pubcode,
                                          listing_url)

                if 'wz.rednet.cn/#/leaveMsgList' in listing_url:
                    section = '问政湖南'
                    REQ_url = "https://wzapi.rednet.cn/front/msg/index"
                    is_reply = re.findall("leaveMsgList\?reply=(.*)", listing_url)
                    if is_reply == []:
                        is_reply = ""
                    else:
                        is_reply = is_reply[0]
                    data = '{"ready":"1","is_reply":"%s","type":"","cate_id":"","pageNum":10,"page":1}' % str(is_reply)
                    Main_json = self.download_page(REQ_url, data, method="POST").json()["data"]["rows"]
                    for Per_json in Main_json:
                        content_url = "https://wzapi.rednet.cn/front/msg/detail?id=" + str(Per_json["id"])
                        timeArray = time.strptime(Per_json["created_at"], "%Y-%m-%d %H:%M:%S")
                        timestamp = int(time.mktime(timeArray))
                        headline = Per_json["title"]
                        author = Per_json["nickname"]
                        content = self.download_page(content_url, method="GET").json()["data"]["content"]
                        content = self.parser_content(content)
                        content_url = "https://people.rednet.cn/#/leaveMsgDetails?id=" + str(Per_json["id"])
                        self.process_data(content_url, headline, section, timestamp, author, content, pubcode,
                                          listing_url)

        except Exception as e:
            logger.info(f"start error: {e}")

        logger.info('end')


if __name__ == '__main__':
    crawl = CrawlBoxun()
    # crawl.start()
    scheduler = BlockingScheduler()
    scheduler.add_job(crawl.start, 'interval', hours=6, next_run_time=datetime.datetime.now())
    scheduler.start()
