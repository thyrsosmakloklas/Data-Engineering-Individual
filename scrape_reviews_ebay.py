# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 17:08:48 2021

@author: Admin
"""

import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime

def get_reviews_ebay(page_url):
    for page in range(1, 4):        
        page_url = page_url[:-15] + str(page)+ page_url[-14:]        
        response = requests.get(page_url)
        html = response.text
        
        soup = BeautifulSoup(html, "html5lib")
        
        review_sections = soup.find_all(class_="ebay-review-section")
        
        #Listings document
        for i, listing in enumerate(review_sections):
            review_text = listing.find(class_="review-item-content").text
            review_text = review_text.replace("Read full review...", "").strip()
            
            author = listing.find(class_="review-item-author").text.strip()
            star_rating = listing.find(class_="star-rating")["aria-label"]
            review_title = listing.find(class_="review-item-title").text.strip()
            
            attributes = listing.find_all(class_="rvw-val")
            try:
                verified_review = attributes[0].text.strip()
                if verified_review.lower == 'yes':
                    condition = attributes[1].text.strip()
                    seller_name = attributes[2].text.strip()
            except:
                pass
            
            source = "ebay"
            
            helpful_upvotes = listing.find(class_='positive-h-c').text
            unhelpful_upvotes = listing.find(class_='negative-h-c').text
        
            date_created = listing.find(class_='review-item-date').text
            datetime_scraped = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
            
            review_id = author + str(star_rating) + str(datetime_scraped)
            
            if 'digital' in page_url.lower():
                model = 'Sony PlayStation 5 Digital Edition'
            else:
                model = 'Sony PlayStation 5 Disc Edition'
        
            review_params = {}
            for variable in ["review_title", "review_text", "author", "star_rating", 
                             "condition", "verified_review", "seller_name", 
                             "helpful_upvotes", "unhelpful_upvotes", "model",
                             "date_created", "datetime_scraped", "source", 
                             "review_id"]:
                review_params[variable] = eval(variable)
                
            review_params = json.loads(json.dumps(review_params), 
                                       parse_float=Decimal)
                
            yield(review_params)
            
            time.sleep(1)

#Digital Edition
url = "https://www.ebay.co.uk/urw/Sony-PS5-Digital-Edition-Console-White/product-reviews/25040975636?pgn=1&condition=all"
for i in get_reviews_ebay(url):
    print(i)

#Disk edition
url = "https://www.ebay.co.uk/urw/Sony-PS5-Blu-Ray-Edition-Console-White/product-reviews/19040936896?pgn=1&condition=all"
for i in get_reviews_ebay(url):
    print(i)