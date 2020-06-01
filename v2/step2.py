from datetime import datetime
import os
import csv
import requests
from lxml import etree
import json
import logging
import threading
import pdb
import time
import random
import multiprocessing.pool as mpool

THREAD_COUNT = 4
RETRY_COUNT = 10

csv_writer_lock = threading.Lock()
session = requests.Session()

session.proxies = {
 "http": "83.149.70.159:13012",
 "https": "83.149.70.159:13012",
}

def validate(item):    
    if item == None:
        item = ''
    if type(item) == int or type(item) == float:
        item = str(item)
    if type(item) == list:
        item = ' '.join(item)
    return item.encode('ascii', 'ignore').decode("utf-8").strip()

def load_blog_urls():
    blogs_file_name = input('Blog file name:')
    if blogs_file_name == '':
        blogs_file_name = 'blogs-raw.csv'
    blog_urls = []
    try:
        with open(blogs_file_name) as csvfile:
            spamreader = csv.reader(csvfile)
            for row in spamreader:
                try:
                    blog_urls.append(validate(row[0]))
                except Exception as e:
                    pass
        return blog_urls[1:]
    except Exception as e:        
        print('File is not exist!')
    return blog_urls

def check_status(url, retry):    
    status = False
    headers = {
        'accept': 'application/json;format=camelcase',
        'accept-language': 'en-us',
        'authorization': 'Bearer aIcXSOoTtqrzR8L8YEIOmBeW94c3FmbSNSWAUbxsny9KKx5VFh',
        'cookie': 'tmgioct=5eb464f70c8a160670060060; euconsent=BOzC_GcOzC_GcAOPoGENC7-AAAAtl6__f_9z_8_v2ddvduz_Ov_j_c_93XW8fPZvcELzhK1Meu_2xxc4u9wNRM5wcgx85eJrEso5YzISsG-RMod_zt__3ziX9oxPowEc9rz3nbEw6vs2v-ZzBCGJ_Iw; yx=9trs7cgs6ipq2%26o%3D3%26f%3Dr0; palette=darkMode; capture=%211231588885784%7CJY2zjXM465s6AymFdzNuqY5X4; devicePixelRatio=0.3333333432674408; pfl=ZDRhMmMyYWFhMmZjZDAxNmI1ZTc0NThlMjNjOWRlNzI3OTg1Y2E2NTBmNDFiNjU3Njc0ZmE4ZjhhODU2MThmZSxyYzI4ZHRmNDhyM2g1ZDdmYnQ0dDh0OXRhNjhtNTk1YywxNTg4ODg1ODYz; documentWidth=2556; pfg=18fdb39e22be77a4ece55944367d9e44596360787290c72a2e8e86e40f26f570%23%7B%22eu_resident%22%3A1%2C%22gdpr_is_acceptable_age%22%3A1%2C%22gdpr_consent_core%22%3A1%2C%22gdpr_consent_first_party_ads%22%3A1%2C%22gdpr_consent_third_party_ads%22%3A1%2C%22gdpr_consent_search_history%22%3A1%2C%22exp%22%3A1620421864%2C%22vc%22%3A%22%22%7D%235544224565; pfp=bsbMaCvFn4QSZe7maka9wROmsFycvfTdNrSIfx1z; pfs=WB9W7hYTeShY1YNP3VUaBuc1A; pfe=1596661877; pfu=369752730; language=%2Cen_US; logged_in=1; redpop=1; sid=alohu7TlemWd3KVt5sl2SeJxOg18eSoyIgRiGOZApdxmdnKKAu.adUmkHvsqDe2TxqVWgzXdCs4dPOF00VMwx0Z9RGqtKlsaLuOGA',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
        'x-version': 'redpop/3/0//redpop/'
    }
    try:
        count = 0
        source = session.get(url, headers=headers).text
        response = json.loads(source)
        if response != None:
            response = response.get('response')
            for post in response.get('posts'):
                if '2020' in post.get('date'):
                    status = True
                    break
    except Exception as e:        
        if retry < RETRY_COUNT:
            retry += 1
            return check_status(url, retry)        
    return status

def get_top_tag_post_count(url, retry):
    count = 0
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'cookie': 'language=%2Cen_US; logged_in=1; tmgioct=5eb582382740a90957499210; pfg=1ff8c1ffd6279195f4726dd820ae987fa66b4dcd93304ddcfa7f6aa8e15e6e15%23%7B%22eu_resident%22%3A1%2C%22gdpr_is_acceptable_age%22%3A1%2C%22gdpr_consent_core%22%3A1%2C%22gdpr_consent_first_party_ads%22%3A1%2C%22gdpr_consent_third_party_ads%22%3A1%2C%22gdpr_consent_search_history%22%3A1%2C%22exp%22%3A1620489656%2C%22vc%22%3A%22%22%7D%239274512003',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
    }
    try:
        response = session.get(url, headers=headers).text        
        data = response.split("window['___INITIAL_STATE___'] =")[-1].split(',"apiHost"')[0] + '}'    
        js_data = json.loads(data)
        count = js_data['tumblelog']['topTags'][0]['count']        
    except Exception as e:        
        if retry < RETRY_COUNT:
            retry += 1
            return get_top_tag_post_count(url, retry)        
    return count

def parse_blog(blog_url):
    domain = blog_url.split('/')[-1].replace('www.', '').split('.')[0]
    url = "https://api.tumblr.com/v2/blog/{}/posts?fields%5Bblogs%5D=avatar%2Cname%2Ctitle%2Curl%2Cupdated%2Cfirst_post_timestamp%2Cposts%2Cdescription".format(domain)
    detail_url = blog_url + '/archive'
    count = get_top_tag_post_count(detail_url, 0)
    status = check_status(url, 0)
    with csv_writer_lock:
        if status == True:
            blogs_active_output_file = open('blogs_active.csv', mode='a+', newline='')
            blogs_active_writer = csv.writer(blogs_active_output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)    
            blogs_active_writer.writerow([blog_url, count])
        else:
            blogs_inactive_output_file = open('blogs-inactive.csv', mode='a+', newline='')
            blogs_inactive_writer = csv.writer(blogs_inactive_output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            blogs_inactive_writer.writerow([blog_url, count])
        print(status, blog_url, count)        

def main():
    blog_urls = load_blog_urls()
    pool = mpool.ThreadPool(THREAD_COUNT)
    for blog_url in blog_urls:
        pool.apply_async(parse_blog, args=(blog_url,))
    pool.close()
    pool.join()


if __name__ == '__main__':
    main()
    