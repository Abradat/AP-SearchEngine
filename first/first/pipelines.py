# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from abc import abstractmethod

import re
from collections import Counter
from scrapy.exceptions import DropItem

import nltk
from stemming.porter2 import stem

import sqlite3


class HovashaPipeline(object):
    @abstractmethod
    def process_item(self, item, spider):
        return item


class DuplicateCheckPipeline(HovashaPipeline):
    def process_item(self, item, spider):
        url = item["url"]

        if self.is_page_exist(url):
            raise DropItem("Duplicate page %s" % url)

        return item

    def is_page_exist(self, url_f):
        with sqlite3.connect('db.db') as conn:
            cursor = conn.execute("SELECT url FROM pages WHERE url=?", (url_f,))
            row = cursor.fetchone()
            if row is None:
                return False
            else:
                return True


class TextNormalizationPipeline(HovashaPipeline):
    def process_item(self, item, spider):
        url = item["url"]
        article = item["article"]

        title = article.title
        main = article.cleaned_text
        title = re.findall(r'[A-Za-z0-9]\w*', title.lower())
        main = re.findall(r'[A-Za-z0-9]\w*', main.lower())

        for i in range(len(main)):
            main[i] = stem(main[i])
        for i in range(len(title)):
            title[i] = stem(title[i])
        delWord = dict(nltk.pos_tag(main))
        for i in delWord:
            if delWord[i] == 'DT' or delWord[i] == 'IN' or delWord[i] == 'CC' or delWord[i] == 'TO':
                for j in range(main.count(i)):
                    main.remove(i)

        delWord = dict(nltk.pos_tag(title))
        for i in delWord:
            if delWord[i] == 'DT' or delWord[i] == 'IN' or delWord[i] == 'CC' or delWord[i] == 'TO':
                for j in range(title.count(i)):
                    title.remove(i)

        main_pos = {}
        for i in range(len(main)):
            if main_pos.get(main[i], 0) == 0:
                main_pos[main[i]] = [i]
            else:
                main_pos[main[i]].append(i)
        main = Counter(main)
        title = Counter(title)

        for i in title:
            title[i] *= 10
        main.update(title)

        return {
            "url": url,
            "title": article.title,
            "content": article.cleaned_text,
            "words": main,
            "wordspos": main_pos}


class StorePipeline(HovashaPipeline):
    def process_item(self, item, spider):
        url = item["url"]
        title = item["title"]
        content = item["content"]
        words = item["words"]


        page = self.store_page(url, title, content)

        for word_text in words:
            score = words[word_text]
            word = self.store_word_if_not_exist(word_text)

            self.make_index(word, page, score)
            #if word_text in words_pos:
            #    self.store_positions(word, page, words_pos[word_text])

    def store_page(self, url, title, content):
        #page = Page(url, title, content)
        with sqlite3.connect('db.db') as conn:
            conn.execute("INSERT INTO pages (url, title, content)"
                         "VALUES (?, ?, ?)", (url, title, content,))
            conn.commit()
        #return page
        return (url, title, content)

    def is_word_exist(self, word_f):
        with sqlite3.connect('db.db') as conn:
            cursor = conn.execute("SELECT word FROM words WHERE word=?", (word_f,))
            row = cursor.fetchone()
            if row is None:
                return False
            else:
                return True

    def store_word_if_not_exist(self, text):

        is_exist = self.is_word_exist(text)
        if not is_exist:
            with sqlite3.connect('db.db') as conn:
                conn.execute("INSERT INTO words (word)"
                             "VALUES (?)", (text,))
            conn.commit()
        return text


    def get_word_id(self, text):
        with sqlite3.connect('db.db') as conn:
            cursor = conn.execute("SELECT id FROM words WHERE word=?", (text,))
            row = cursor.fetchone()
            if row is None:
                return False
            else:
                return row[0]

    def get_page_id(self, url_f):
        with sqlite3.connect('db.db') as conn:
            cursor = conn.execute("SELECT id FROM pages WHERE url=?", (url_f,))
            row = cursor.fetchone()
            if row is None:
                return False
            else:
                return row[0]


    def make_index(self, word, page, score):
        word_id = self.get_word_id(word)
        page_id = self.get_page_id(page[0])

        with sqlite3.connect('db.db') as conn:
            conn.execute("INSERT INTO indexes (word_id, page_id, score)"
                         "VALUES (?, ?, ?)", (word_id, page_id, score,))
            conn.commit()
