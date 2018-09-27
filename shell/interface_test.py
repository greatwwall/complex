# -*- coding:utf-8 -*-

import urllib.request
import urllib.parse
import http.client
import json

def main():
    all_get()
    all_post()
    
# 所有get接口调用    
def all_get():
    get('action/test', 'a=1&b=2')
 
 # 所有post接口调用
def all_post():
    post('action/input', {"a": "1", "b": "2"})

# post接口执行方法
def post(url, data):
    prefix = 'http://localhost:8083/'
    conn = httpclient.HTTPConnection('localhost', 8083)
    post_data = json.dumps(data).encode('utf-8')
    header = {'Content-Type' : 'application/json'}
    
    conn.request('POST', prefix + url, post_data, header)
    result = conn.getresponse()
    print('POST: ' + prefix + url)
    print('PAR : ' + str(data))
    res = json.loads(result.read())
    if res.get('code') == '000000':
        print('RES : success')
    else:
        print('RES : ' + res.get('msg'))
    print()
    
# get接口执行方法
def get(url, data=''):
    prefix = 'http://localhost:8080'
    print('GET : ' + prefix + url)
    if data != '':
        url = url + '?' + data
        print('PAR : ' + data)
    result = urllib.request.urlopen(prefix + url)
    
    res = json.loads(result.read().decode('utf-8'))
    if res.get('code') == '000000':
        print('RES : success')
    else: 
        print('RES : ' + res.get('msg'))
    print()
    
if __name__ == '__main__':
    main()
