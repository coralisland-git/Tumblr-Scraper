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
import shutil

csv_writer_lock = threading.Lock()

THREAD_COUNT = 10
PHOTO_COUNT = 10
RETRY_COUNT = 10
NEXT_PAGE_MAX_COUNT = 50

ROOT_PATH = './photos'
if not os.path.isdir(ROOT_PATH):
   os.makedirs(ROOT_PATH)

photo_count = input("Photo count:")
if photo_count == "":
    photo_count = PHOTO_COUNT
else:
    try:
        photo_count = int(photo_count)
    except Exception as e:
        photo_count = 0    
        print('Invalid format')

PROXY_LIST = []
proxy_idx = 0
try:
    proxy_file_name = input("Proxy file name:")
    if proxy_file_name == "":
        proxy_file_name = 'proxies300.txt'
    with open(proxy_file_name, 'rb') as text:
        PROXY_LIST =  [x.strip().decode('utf8') for x in text.readlines()]
    proxy_count = len(PROXY_LIST)
    if proxy_count == 0:
        print("Proxy is required")
        exit(0)
except Exception as e:
    print('Proxy list is not exist!')
    exit(0)

session = requests.Session()
# session.proxies = {
#  "http": "83.149.70.159:13012",
#  "https": "83.149.70.159:13012",
# }

headers = {
    'accept': 'application/json;format=camelcase',
    'accept-language': 'en-us',
    'authorization': 'Bearer aIcXSOoTtqrzR8L8YEIOmBeW94c3FmbSNSWAUbxsny9KKx5VFh',
    'cookie': 'tmgioct=5eb464f70c8a160670060060; euconsent=BOzC_GcOzC_GcAOPoGENC7-AAAAtl6__f_9z_8_v2ddvduz_Ov_j_c_93XW8fPZvcELzhK1Meu_2xxc4u9wNRM5wcgx85eJrEso5YzISsG-RMod_zt__3ziX9oxPowEc9rz3nbEw6vs2v-ZzBCGJ_Iw; yx=9trs7cgs6ipq2%26o%3D3%26f%3Dr0; palette=darkMode; capture=%211231588885784%7CJY2zjXM465s6AymFdzNuqY5X4; devicePixelRatio=0.3333333432674408; pfl=ZDRhMmMyYWFhMmZjZDAxNmI1ZTc0NThlMjNjOWRlNzI3OTg1Y2E2NTBmNDFiNjU3Njc0ZmE4ZjhhODU2MThmZSxyYzI4ZHRmNDhyM2g1ZDdmYnQ0dDh0OXRhNjhtNTk1YywxNTg4ODg1ODYz; documentWidth=2556; pfg=18fdb39e22be77a4ece55944367d9e44596360787290c72a2e8e86e40f26f570%23%7B%22eu_resident%22%3A1%2C%22gdpr_is_acceptable_age%22%3A1%2C%22gdpr_consent_core%22%3A1%2C%22gdpr_consent_first_party_ads%22%3A1%2C%22gdpr_consent_third_party_ads%22%3A1%2C%22gdpr_consent_search_history%22%3A1%2C%22exp%22%3A1620421864%2C%22vc%22%3A%22%22%7D%235544224565; pfp=bsbMaCvFn4QSZe7maka9wROmsFycvfTdNrSIfx1z; pfs=WB9W7hYTeShY1YNP3VUaBuc1A; pfe=1596661877; pfu=369752730; language=%2Cen_US; logged_in=1; redpop=1; sid=alohu7TlemWd3KVt5sl2SeJxOg18eSoyIgRiGOZApdxmdnKKAu.adUmkHvsqDe2TxqVWgzXdCs4dPOF00VMwx0Z9RGqtKlsaLuOGA',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
    'x-version': 'redpop/3/0//redpop/'
}

img_headers = {
    'accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
    'accept-encoding': 'gzip, deflate, br',
    'cookie': 'palette=trueBlue',
    'cookie': 'tmgioct=5eb464f70c8a160670060060; euconsent=BOzC_GcOzC_GcAOPoGENC7-AAAAtl6__f_9z_8_v2ddvduz_Ov_j_c_93XW8fPZvcELzhK1Meu_2xxc4u9wNRM5wcgx85eJrEso5YzISsG-RMod_zt__3ziX9oxPowEc9rz3nbEw6vs2v-ZzBCGJ_Iw; yx=9trs7cgs6ipq2%26o%3D3%26f%3Dr0; palette=darkMode; capture=%211231588885784%7CJY2zjXM465s6AymFdzNuqY5X4; devicePixelRatio=0.3333333432674408; pfl=ZDRhMmMyYWFhMmZjZDAxNmI1ZTc0NThlMjNjOWRlNzI3OTg1Y2E2NTBmNDFiNjU3Njc0ZmE4ZjhhODU2MThmZSxyYzI4ZHRmNDhyM2g1ZDdmYnQ0dDh0OXRhNjhtNTk1YywxNTg4ODg1ODYz; documentWidth=2556; pfg=18fdb39e22be77a4ece55944367d9e44596360787290c72a2e8e86e40f26f570%23%7B%22eu_resident%22%3A1%2C%22gdpr_is_acceptable_age%22%3A1%2C%22gdpr_consent_core%22%3A1%2C%22gdpr_consent_first_party_ads%22%3A1%2C%22gdpr_consent_third_party_ads%22%3A1%2C%22gdpr_consent_search_history%22%3A1%2C%22exp%22%3A1620421864%2C%22vc%22%3A%22%22%7D%235544224565; pfp=bsbMaCvFn4QSZe7maka9wROmsFycvfTdNrSIfx1z; pfs=WB9W7hYTeShY1YNP3VUaBuc1A; pfe=1596661877; pfu=369752730; language=%2Cen_US; logged_in=1; redpop=1; sid=alohu7TlemWd3KVt5sl2SeJxOg18eSoyIgRiGOZApdxmdnKKAu.adUmkHvsqDe2TxqVWgzXdCs4dPOF00VMwx0Z9RGqtKlsaLuOGA',
    'referer': 'https://66.media.tumblr.com/7f5a89794b377c24618a532bbac105b4/b597abe11f4f3791-65/s1280x1920/e2348eda867fa6484ed7f092a72e01308b9725a2.jpg',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
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
    blogs_file_name = input("Blog file name:")
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
    return load_blog_urls

def gen_file_index_name(idx):
    idx = str(idx)
    return '0' * (4 - len(idx)) + idx

def get_image_url(content):
    size_list = [1080, 1280]
    image_url = None
    scraped = False
    for item in content:
        if scraped == True:
            break
        if item.get('type') != 'image':
            continue
        for size in size_list:
            if image_url != None:
                break
            for image in item.get('media'):
                width = image.get('width')
                if width == size:
                    image_url = validate(image.get('url'))
                    scraped = True
                    break
        if image_url == None:
            for image in item.get('media'):
                width = image.get('width')
                if width < 1080:
                    image_url = validate(image.get('url'))
                    scraped = True
                    break
    return image_url

def parse_post(blog_path, file_name, image_url):
    global  proxy_idx
    for idx in range(RETRY_COUNT):
        try:
            proxy_idx += 1
            proxy = PROXY_LIST[proxy_idx%proxy_count]
            file_content = session.get(image_url, 
                headers=img_headers, 
                stream=True,
                proxies={
                    "http": proxy,
                    "https": proxy,
                }
            )
            with open('{}/{}'.format(blog_path, file_name), mode='wb') as output:
                file_content.raw.decode_content = True
                shutil.copyfileobj(file_content.raw, output)
            print(file_name)
            break
        except Exception as e:
            continue


def parse_blog(domain , url, count, retry, next_count):
    global  proxy_idx
    try:
        blog_path = '{}/{}'.format(ROOT_PATH, domain)
        if not os.path.isdir(blog_path):
            os.makedirs(blog_path)
        blog_output_file = open('{}/{}.csv'.format(blog_path, domain), mode='a+', newline="")
        blog_writer = csv.writer(blog_output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        response = None
        try:
            proxy_idx += 1
            proxy = PROXY_LIST[proxy_idx%proxy_count]
            data = session.get(url, headers=headers, proxies={
                "http": proxy,
                "https": proxy,
            }).text
            response = json.loads(data)
        except:            
            if retry < RETRY_COUNT:
                retry += 1
                parse_blog(domain, url, count, retry, 0)
        if response != None:
            response = response.get('response')
            next_url = 'https://api.tumblr.com'+response.get('links').get('next').get('href')
            ch_pool = mpool.ThreadPool(THREAD_COUNT)
            for post in response.get('posts'):
                if count > photo_count:
                    break
                content = post.get('content')
                if len(content) == 0:
                    trails = post.get('trail')
                    if len(trails) > 0:
                        content = trails[0].get('content')
                image_url = get_image_url(content)
                if image_url == None:
                    continue
                file_name = '{}-{}.jpg'.format(domain, gen_file_index_name(count))
                tags = validate(', '.join(post.get('tags')))
                with csv_writer_lock:
                    blog_writer.writerow([file_name, tags])
                ch_pool.apply_async(parse_post, args=(blog_path, file_name, image_url))
                count += 1
            ch_pool.close()
            ch_pool.join()
            if count < photo_count and next_url != None and next_count < NEXT_PAGE_MAX_COUNT:
                next_count += 1
                parse_blog(domain, next_url, count, 0, next_count)
    except Exception as e:        
        print(e)

def main():
    blog_urls = load_blog_urls()
    pool = mpool.ThreadPool(THREAD_COUNT)
    for blog_url in blog_urls:
        domain = blog_url.split('/')[-1].replace('www.', '').split('.')[0]
        if domain != '':
            url = "https://api.tumblr.com/v2/blog/{}/posts?fields%5Bblogs%5D=avatar%2Cname%2Ctitle%2Curl%2Cdescription_npf%2Ctheme%2Cuuid%2Ccan_message%2Ccan_be_followed%2C%3Ffollowed%2C%3Fis_member%2C%3Fprimary%2Cshare_likes%2Cshare_following%2Ccan_subscribe%2Csubscribed%2Cask%2C%3Fcan_submit%2C%3Fis_blocked_from_primary%2C%3Ftweet%2Cupdated%2Cfirst_post_timestamp%2Cposts%2Cdescription&npf=true&reblog_info=true&type=photo".format(domain)
        pool.apply_async(parse_blog, args=(domain , url, 1, 0, 0, ))                
    pool.close()
    pool.join()

if __name__ == '__main__':
    main()
    