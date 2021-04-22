# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 00:44:42 2021

@author: Thyrsos Makloklas
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
from datetime import datetime
from decimal import Decimal
import json

#product features
def get_product_features(link):
    response = requests.get(link)
    html = response.text
    soup = BeautifulSoup(html, "html5lib")
    
    item_id = (link.split("hash=item"))[1].split(":g:")[0]
    try:
        seller_name = soup.find(class_="mbg-nw").text
    except:
        seller_name = soup.find(class_="mbg").text
    
    try:
        seller_pos_feedback_12m_per = float(soup.find(id="si-fb").text.split("%")[0])
    except:
        pass
    
    try:
        product_rating_avg = float(soup.find(class_="ebay-review-start-rating").text.split("\t")[-1])
    except:
        pass
        
    attr_table = soup.find(class_="itemAttr")
    attributes = []
    for el in attr_table.find_all(class_="attrLabels"):
            attributes.append(el.text.strip().replace(":", "").lower())
            
    len_attrs = len(attributes)
    
    dict_features = {}
    for i, el in enumerate(list(attr_table.find_all("span"))[-len_attrs:]):
        try:
            dict_features[attributes[i]] = el.text
        except:
            pass
    
    if 'condition' in map(str.lower,attributes):
        try:
            del dict_features['condition']
        except:
            del dict_features['Condition']
    
    for variable in ["item_id", "seller_name", "seller_pos_feedback_12m_per",
                     "product_rating_avg"]:
        try:
            dict_features[variable] = eval(variable)
        except:
            pass
    
    return(dict_features)

def get_products(page_url):
    for page in range(1, 10):
        page_url = page_url[0:-1] + str(page)
        url = page_url
        response = requests.get(url)
        html = response.text
        
        soup = BeautifulSoup(html, "html5lib")
        
        #Listings document
        for listing in soup.find_all(class_=("s-item--watch-at-corner")):
            
            listing_title = listing.find("h3", class_=("s-item__title")).text.strip()
            listing_title = listing_title.replace("New listing", "")
            
            if str(5) not in listing_title:
                pass
            
            price = re.split("£|x", listing.find(class_=("s-item__detail s-item__detail--primary")).text)[1]
            photo_url = listing.find(class_="s-item__image-img")["src"]
            condition = listing.find(class_="SECONDARY_INFO").text
            item_link = listing.find(class_="s-item__link")["href"]
            datetime_scraped = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
                        
            listing_features = {}
            for variable in ["listing_title", "price", "photo_url", 
                             "condition", "item_link", 'datetime_scraped']:
                listing_features[variable] = eval(variable)
            
            product_features = get_product_features(item_link)
            
            output = {**listing_features, **product_features}
            output = json.loads(json.dumps(output), parse_float=Decimal)
            
            time.sleep(1)
            
            yield(output)
    
url = "https://www.ebay.co.uk/sch/i.html?_from=R40&_nkw=ps5+console&_sacat=0&LH_TitleDesc=0&_pgn=1"

for i in get_products(url):
    print(i)