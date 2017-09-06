# -*- coding: UTF-8 -*-

import ConfigParser


class ConfParser(object):
    def __init__(self, path):
        self.kv = {}
        self.conf = ConfigParser.ConfigParser()
        self.conf.read(path)
        self.load('common')

    def load(self, section):
        if not section:
            return
        for option in self.conf.options(section):
            self.kv[option] = self.conf.get(section, option)

    def load_to_dict(self, section):
        params = dict()
        if not section:
            return params
        for option in self.conf.options(section):
            self.kv[option] = self.conf.get(section, option)
            params[option] = self.conf.get(section, option)
        return params

    def build(self, layer, raw_feature_name = None):
        self.kv.clear()
        self.load('common')
        self.load(raw_feature_name)
        self.load(layer)

    def get(self, key):
        return self.kv[key]

    def has(self, key):
        return self.kv.has_key(key)

if __name__ == '__main__':
    conf = ConfParser('../../conf/classify.conf')
    conf.build('layer1', 'n_gram')
    print conf.kv
    conf.build('layer1')
    print conf.kv
