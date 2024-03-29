import time
import random

import scrapy
from scrapy.http import Response
from dotenv import dotenv_values


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    nexPageSelector = '.next::attr(href)'
    itemproc = None
    firstPage = None

    def start_requests(self):
        self.itemproc = self.crawler.engine.scraper.itemproc
        self.firstPage = dotenv_values().get('FIRST_PAGE')

        yield scrapy.Request(url=self.firstPage, callback=self.parse)

    def parse(self, response: Response):
        # We do things here

        for article in response.css('article'):
            self.itemproc.process_item(self.createItemFromArticle(article), self)

        # Go to next page
        return self.get_next_page(response)

    def get_next_page(self, response: Response):
        next_page = response.css(self.nexPageSelector).get()

        if next_page is not None:
            self.log(next_page)
            next_page = response.urljoin(next_page)
            # self.random_wait()
            yield scrapy.Request(url=next_page, callback=self.parse)

    def random_wait(self):
        random_milliseconds = (random.uniform(300, 2999)) / 1000.0
        self.log('waiting ' + str(random_milliseconds))
        time.sleep(random_milliseconds)

    def createItemFromArticle(self, article):
        return {
            'id': article.css('article::attr(id)').get().split('-')[1],
            'url': article.css('figcaption a::attr(href)').get().strip(),
            'title': article.css('figcaption a::text').get().strip(),
            'img': article.css('figure .data-bg-hover.data-bg.data-bg-categorised::attr(data-background)')
            .get().strip()
        }
