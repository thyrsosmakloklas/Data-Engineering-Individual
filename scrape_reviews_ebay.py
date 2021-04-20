# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 17:08:48 2021

@author: Admin
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

#Disk Edition
url = "https://www.ebay.co.uk/urw/Sony-PS5-Blu-Ray-Edition-Console-White/product-reviews/19040936896?pgn=1&condition=all"
response = requests.get(url)
html = response.text

soup = BeautifulSoup(html, "html5lib")

review_sections = soup.find_all(class_="ebay-review-section")

for i, listing in enumerate(review_sections):
    review_text = listing.find(class_="review-item-content").text
    review_text = review_text.replace("Read full review...", "").strip()
    
    author = listing.find(class_="review-item-author").text.strip()
    star_rating = listing.find(class_="star-rating")["aria-label"]
    review_title = listing.find(class_="review-item-title").text.strip()
    
    attributes = listing.find_all(class_="rvw-val")
    verified_review = attributes[0].text.strip()
    condition = attributes[1].text.strip()
    seller_name = attributes[2].text.strip()

    helpful_unhelpful = [listing.find(class_='positive-h-c').text, 
                     listing.find(class_='negative-h-c').text]
    
    date = listing.find(class_='review-item-date').text
    

    a_dict = {}
    
    for variable in ["review_title", "review_text", "author", "star_rating", "condition",
                     "attributes", "verified_review", "seller_name", 
                     "helpful_unhelpful", "date"]:
        a_dict[variable] = eval(variable)
    
    print(a_dict)

#Digital Edition
url = "https://www.ebay.co.uk/urw/Sony-PS5-Digital-Edition-Console-White/product-reviews/25040975636?condition=all"
response = requests.get(url)
html = response.text

soup = BeautifulSoup(html, "html5lib")

review_sections = soup.find_all(class_="ebay-review-section")

for i, listing in enumerate(review_sections):
    review_text = listing.find(class_="review-item-content").text
    review_text = review_text.replace("Read full review...", "").strip()
    
    author = listing.find(class_="review-item-author").text.strip()
    star_rating = listing.find(class_="star-rating")["aria-label"]
    
    attributes = listing.find_all(class_="rvw-val")
    verified_review = attributes[0].text.strip()
    condition = attributes[1].text.strip()
    seller_name = attributes[2].text.strip()

    helpful_unhelpful = [listing.find(class_='positive-h-c').text, 
                     listing.find(class_='negative-h-c').text]
    
    date = listing.find(class_='review-item-date').text
    

    a_dict = {}
    
    for variable in ["review_text", "author", "star_rating", "condition",
                     "attributes", "verified_review", "seller_name", 
                     "helpful_unhelpful", "date"]:
        a_dict[variable] = eval(variable)
    
    print(a_dict)