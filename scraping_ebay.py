# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 00:44:42 2021

@author: Thyrsos Makloklas
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

url = "https://www.ebay.co.uk/sch/i.html?_from=R40&_nkw=ps5+console&_sacat=0&_pgn=1https://www.ebay.co.uk/sch/i.html?_from=R40&_nkw=ps5+console&_sacat=0&_pgn=1"
response = requests.get(url)
html = response.text

soup = BeautifulSoup(html, "html5lib")

items = soup.select(".s-item")

#Listings document
for i, listing in enumerate(soup.find_all("li", class_=("s-item s-item--watch-at-corner"))):
    
    listing_title = listing.find("h3", class_=("s-item__title")).text
    price = re.split("£|x", listing.find(class_=("s-item__detail s-item__detail--primary")).text)[1]
    photo_url = listing.find(class_="s-item__image-img")["src"]
    condition = listing.find(class_="SECONDARY_INFO").text
    item_link = listing.find(class_="s-item__link")["href"]
    
    a_dict = {}
    
    for variable in ["listing_title", "price", "photo_url", 
                     "condition", "item_link"]:
        a_dict[variable] = eval(variable)
    
    print(a_dict)

#product features
item_link
response = requests.get("https://www.ebay.co.uk/itm/Sony-PS5-Blu-Ray-Edition-Console-White/284254935995?epid=19040936896&hash=item422eea1bbb:g:~zYAAOSwnFBgZd2p")
html = response.text

soup = BeautifulSoup(html, "html5lib")

seller_name = soup.find(class_="mbg-nw").text
seller_pos_feedback_12m = int(soup.find(id="si-fb").text.split("%")[0])
product_rating_avg = float(soup.find(class_="ebay-review-start-rating").text.split("\t")[-1])

#buy or bid
buy = soup.find(id="binBtn_btn").text.split("\t")[-1]
try:
    bid = soup.find(id="bidBtn_btn").text.split("\t")[-1]
except: 
    pass

if ('bid' in locals() and 'buy' in locals()):
    sell_method = "buy_bid"
elif 'buy' in locals:
    sell_method = "buy"
elif 'bid' in locals:
    sell_method = "bid"

#bid price    
try:
    bid_last_price = float(re.sub('[^0-9.]', '', soup.find(id='convbidPrice').text))

#buy now price
try:
    buy_last_price = float(re.sub("[^0-9.]", "", soup.find(id="prcIsum").text))

condition = soup.find(id="vi-itm-cond").text

#table for features
a = soup.find(class_="itemAttr")

head = body[0]
body = a.find_all("td")
body_rows = body[1:]

a.find_all(class_="attrLabels")

attributes = []
for el in a.find_all(class_="attrLabels"):
        attributes.append(el.get_text().strip().replace(":", ""))
        
len_attrs = len(attributes)

for el in list(a.find_all("span"))[-len_attrs:]:
    print(el.text.strip())