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


blog_raw_output_file = open('blogs-raw.csv', mode='a+', newline='')
blog_writer = csv.writer(blog_raw_output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
hashtags_output_file = open('hashtags.csv', mode='a+', newline='')
hashtags_writer = csv.writer(hashtags_output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)

THREAD_COUNT = 4
PAGE_LIMIT = 20

history = []

session = requests.Session()

session.proxies = {
 "http": "83.149.70.159:13012",
 "https": "83.149.70.159:13012",
}

headers = {
    'accept': 'application/json;format=camelcase',
    'accept-language': 'en-us',
    'authorization': 'Bearer aIcXSOoTtqrzR8L8YEIOmBeW94c3FmbSNSWAUbxsny9KKx5VFh',
    'cookie': 'tmgioct=5eb464f70c8a160670060060; euconsent=BOzC_GcOzC_GcAOPoGENC7-AAAAtl6__f_9z_8_v2ddvduz_Ov_j_c_93XW8fPZvcELzhK1Meu_2xxc4u9wNRM5wcgx85eJrEso5YzISsG-RMod_zt__3ziX9oxPowEc9rz3nbEw6vs2v-ZzBCGJ_Iw; yx=9trs7cgs6ipq2%26o%3D3%26f%3Dr0; palette=darkMode; capture=%211231588885784%7CJY2zjXM465s6AymFdzNuqY5X4; devicePixelRatio=0.3333333432674408; pfl=ZDRhMmMyYWFhMmZjZDAxNmI1ZTc0NThlMjNjOWRlNzI3OTg1Y2E2NTBmNDFiNjU3Njc0ZmE4ZjhhODU2MThmZSxyYzI4ZHRmNDhyM2g1ZDdmYnQ0dDh0OXRhNjhtNTk1YywxNTg4ODg1ODYz; documentWidth=2556; pfg=18fdb39e22be77a4ece55944367d9e44596360787290c72a2e8e86e40f26f570%23%7B%22eu_resident%22%3A1%2C%22gdpr_is_acceptable_age%22%3A1%2C%22gdpr_consent_core%22%3A1%2C%22gdpr_consent_first_party_ads%22%3A1%2C%22gdpr_consent_third_party_ads%22%3A1%2C%22gdpr_consent_search_history%22%3A1%2C%22exp%22%3A1620421864%2C%22vc%22%3A%22%22%7D%235544224565; pfp=bsbMaCvFn4QSZe7maka9wROmsFycvfTdNrSIfx1z; pfs=WB9W7hYTeShY1YNP3VUaBuc1A; pfe=1596661877; pfu=369752730; language=%2Cen_US; logged_in=1; redpop=1; sid=alohu7TlemWd3KVt5sl2SeJxOg18eSoyIgRiGOZApdxmdnKKAu.adUmkHvsqDe2TxqVWgzXdCs4dPOF00VMwx0Z9RGqtKlsaLuOGA',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    'x-version': 'redpop/3/0//redpop/'
}

def validate(item):    
    if item == None:
        item = ''
    if type(item) == int or type(item) == float:
        item = str(item)
    if type(item) == list:
        item = ' '.join(item)
    return item.encode('ascii', 'ignore').decode("utf-8").strip()

def load_hashtags():
    hashtags_file_name = input('Hashtag file name:')
    if hashtags_file_name == '':
        hashtags_file_name = 'hashtags-sample.csv'
    hashtags = []
    try:
        with open(hashtags_file_name) as csvfile:
            spamreader = csv.reader(csvfile)
            for row in spamreader:            
                try:
                    hashtag = validate(row[0]).replace('#', '')
                    if hashtag != '':
                        hashtags.append(hashtag)
                except Exception as e:
                    pass
    except Exception as e:
        print('File is not exist!')    
    return hashtags

def parse_page(url, cnt):
    source = session.get(url, headers=headers).text        
    next_url = None
    try:
        response = json.loads(source)
        if response != None:
            response = response.get('response')
            if 'posts' in response:                
                posts = response.get('posts')
                if 'links' in posts:
                    next_url = 'https://www.tumblr.com/api'+posts.get('links').get('next').get('href')
                for post in posts.get('data'):
                    post_url = validate(post.get('postUrl')).split('/post')[0]                    
                    if post_url not in history:
                        history.append(post_url)                        
                        blog_writer.writerow([post_url])
                    hash_tags = validate(', '.join(post.get('tags')))
                    hashtags_writer.writerow([post_url, hash_tags])
                    # print([post_url, hash_tags])
            elif 'blogs' in response:
                blogs = response.get('blogs')
                if 'links' in blogs:
                    next_url = 'https://www.tumblr.com/api'+blogs.get('links').get('next').get('href')
                for blog in blogs.get('data'):
                    if blog.get('isAdult') == False:
                        for post in blog.get('posts'):
                            post_url = validate(post.get('postUrl')).split('/post')[0]                            
                            if post_url not in history:
                                history.append(post_url)
                                blog_writer.writerow([post_url])
                            hash_tags = validate(', '.join(post.get('tags')))
                            hashtags_writer.writerow([post_url, hash_tags])
                            print([post_url, hash_tags])
            cnt += 1
            if cnt < PAGE_LIMIT and next_url != None:
                parse_page(next_url, cnt)
    except Exception as e:
        pass

def main():
    hashtags = load_hashtags()
    pool = mpool.ThreadPool(THREAD_COUNT)    
    for hashtag in hashtags:
        url = """https://www.tumblr.com/api/v2/mobile/search?blog_limit=10&post_limit=0&query={}&mode=top&fields%5Bblogs%5D=name%2Cavatar%2Ctheme%2Ctitle%2Curl%2Cuuid%2C%3Ffollowed%2Ccan_message%2Ccan_be_followed%2Cis_adult%2C%3Fis_member%2Cdescription_npf%2Cshare_following%2Cshare_likes%2Cask&reblog_info=true&blog_offset=11""".format(hashtag)
        pool.apply_async(parse_page, args=(url, 0))
    pool.close()
    pool.join()

if __name__ == '__main__':
    main()