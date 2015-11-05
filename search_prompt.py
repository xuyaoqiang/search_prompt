#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: xuyaoqiang
@contact: xuyaoqiang@gmail.com
@date: 2015-07-09 21:46
@version: 0.0.0
@license:
@copyright:

"""
try:
    import json 
except:
    import simplejson  as json 
import jieba
import hashlib
import redis 
from pypinyin import lazy_pinyin
jieba.initialize()

class SearchPrompt(object):

    def __init__(self, scope='sprompt', redis_addr='localhost'):
        self.scope = scope
        self.redis = redis.Redis(redis_addr) 
        self.db = "db:%s" % scope
        self.index = "index:%s" % scope
        self.hot = "hot:%s" % scope 

    def _get_index_key(self, key):
        return "%s:%s" % (self.index, key)

    def item_check(self, item):
        if 'term' not in item:
            raise Exception("Item should have key term")

    def prefixs_for_term(self, term, seg=False):
        term = term.lower()
        prefixs = []

        for index, word in enumerate(term):
            prefixs.append(term[:index+1])
        if seg: 
            words = jieba.cut(term)
            for word in words:
                prefixs.append(word)

        return prefixs

    def _index_prefix(self, prefix, uid, score=0):
        self.redis.sadd(self.index, prefix)
        self.redis.zadd(self._get_index_key(prefix), uid, score)


    def add(self, item, pinyin=False, seg=False):
        self.item_check(item)
        term = item['term']
        score = item.get('score', 0)
        uid = hashlib.md5(item['term'].encode('utf8')).hexdigest()
        
        self.redis.hset(self.db, uid, json.dumps(item))
        for prefix in self.prefixs_for_term(term, seg):
            self._index_prefix(prefix, uid, score=score)
            if pinyin:
                prefix_pinyin = ''.join(lazy_pinyin(prefix))
                self._index_prefix(prefix_pinyin, uid, score=score)


    def _delete_prefix(self, prefix, uid):
        self.redis.zrem(self._get_index_key(prefix), uid)
        if not self.redis.zcard(self._get_index_key(prefix)):
            self.redis.delete(self._get_index_key(prefix))
            self.redis.srem(self.index, prefix)


    def delete(self, item, pinyin=False, seg=False):
        self.item_check(item)
        uid = hashlib.md5(item['term'].encode('utf8')).hexdigest()
        for prefix in self.prefixs_for_term(item['term'], seg=seg):
            self._delete_prefix(prefix, uid)
            if pinyin:
                prefix_pinyin = ''.join(lazy_pinyin(prefix))
                self._delete_prefix(prefix_pinyin, uid)


    def update(self, item):
        self.delete(item)
        self.add(item)

    def normalize(self, prefix):
        words = jieba.cut(prefix)
        return [w for w in words if w.strip()]

    def _search(self, query, limit=5, fuzzy=False):
        query = query.lower() 
        if not query: return []
        if fuzzy:
            search_querys = self.normalize(query) 
        else:
            search_querys = [query]
        self.redis.zincrby(self.hot, query, 1)
        cache_key = self._get_index_key(('|').join(search_querys)) 
        if not self.redis.exists(cache_key):
            self.redis.zinterstore(cache_key, 
                    map(lambda x:self._get_index_key(x), search_querys))
        ids = self.redis.zrevrange(cache_key, 0, limit)
        if not ids: return ids
        return map(lambda x:json.loads(x), self.redis.hmget(self.db, *ids))
        
    def search(self, query, limit=5, fuzzy=False):
        ids = []
        query = query.lower()
        if not query: return ids 
       
        ids = self.redis.zrevrange(self._get_index_key(query), 0, limit-1)
        self.redis.zincrby(self.hot, query, 1)

        if fuzzy:
            segments = self.normalize(query)
            if len(ids) < limit and len(segments) > 1:
                inter_cache_key = self._get_index_key(('&').join(segments)) 
                if not self.redis.exists(inter_cache_key):
                    self.redis.zinterstore(inter_cache_key, 
                        map(lambda x:self._get_index_key(x), segments))
                inter_ids = self.redis.zrevrange(inter_cache_key, 0, limit)
                self.redis.delete(inter_cache_key)

                union_cache_key = self._get_index_key(('|').join(segments))
                if not self.redis.exists(union_cache_key):
                    self.redis.zunionstore(union_cache_key, 
                        map(lambda x:self._get_index_key(x), segments))
                union_ids = self.redis.zrevrange(union_cache_key, 0, limit)
                self.redis.delete(union_cache_key)

                ids.extend(list(set(inter_ids) - set(ids)))
                ids.extend(list(set(union_ids) - set(ids)))
        if not ids: return ids 
        ids = ids[:limit]
        return map(lambda x:json.loads(x), self.redis.hmget(self.db, *ids))
        

    def hot_query(self, limit=5):
        querys = self.redis.zrevrange(self.hot, 0, limit)
        return querys 

