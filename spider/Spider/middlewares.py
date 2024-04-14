# 中间件文件
import json
import urllib

from scrapy import signals

from itemadapter import is_item, ItemAdapter

class SpiderSpiderMiddleware:

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        return None

    def process_spider_output(self, response, result, spider):
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        pass

    def process_start_requests(self, start_requests, spider):
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class SpiderDownloaderMiddleware:

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        return None

    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

import random
from scrapy import signals
from scrapy.exceptions import NotConfigured

class RandomProxyMiddleware(object):

    def __init__(self, settings):
        self.current_proxy = None

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def process_request(self, request, spider):
        if self.current_proxy is None:
            self.current_proxy = self.request_proxy()
        request.meta['proxy'] = self.current_proxy

    def process_response(self, request, response, spider):
        if response.status in [403, 429]:
            self.current_proxy = self.request_proxy()
            new_request = request.copy()
            new_request.dont_filter = True
            return new_request
        return response


    def request_proxy(self):
        url = '&transferip=1'

        try:
            # 发起GET请求
            response = urllib.request.urlopen(url)
            # 读取响应内容
            content = response.read()
            # 打印响应内容
            data = json.loads(content.decode('utf-8'))
            proxy = ""+data['data']['proxy_list'][0]
            print("proxy", proxy)
            return  proxy

        except urllib.error.URLError as e:
            print("请求发生错误:", e)

    def spider_closed(self):
        self.current_proxy = None