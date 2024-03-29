# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector


class MyScraperPipeline:
    def process_item(self, item, spider):
        return item


class MysqlPipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='scrapy'
        )

        self.cur = self.conn.cursor()  # Create cursor to execute sql commands

        self.create_tables()

        self.previous = self.get_previous()

    def create_tables(self):
        self.cur.execute("""
               CREATE TABLE IF NOT EXISTS quotes(
                   id int NOT NULL Primary KEY, 
                   title text,
                   url text,
                   img text
               )
               """)

        self.cur.execute("""
               CREATE TABLE IF NOT EXISTS meta(
                   name VARCHAR(45) NOT NULL PRIMARY KEY, 
                   value VARCHAR(45) NOT NULL
               )
               """)

    def get_previous(self):
        self.cur.execute(""" Select * from meta where name = "last_id" """)
        previous = self.cur.fetchone()

        if previous is None:
            self.cur.execute(""" insert into meta (name, value) values (%s, %s)""", (
                "last_id",
                0,
            ))

            self.cur.execute(""" Select * from meta where name = "last_id" """)
            previous = self.cur.fetchone()

        return previous

    def process_item(self, item, spider):
        # Define insert statement
        try:
            self.cur.execute(""" insert into quotes (id, title, url, img) values (%s,%s,%s,%s)""", (
                item["id"],
                item["title"],
                item["url"],
                item["img"]
            ))

            # self.cur.execute(""" update meta set value = %s where name = %s """, (
            #     item["id"],
            #    "last_id",
            # ))

            self.conn.commit()
        except Exception as e:
            spider.crawler.engine.close_spider(spider, reason='duplicated item found')

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
