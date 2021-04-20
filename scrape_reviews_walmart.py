# -*- coding: utf-8 -*-
"""
Created on Tue Apr 20 16:27:19 2021

@author: Admin
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

#Disk Edition
url = "https://www.walmart.com/reviews/product/363472942"
response = requests.get(url)
html = response.text

soup = BeautifulSoup(html, "html5lib")

review_sections = soup.select(".Grid.ReviewList-content")

for i, listing in enumerate(review_sections):
    review_text = listing.find(class_="review-text").text
    review_text = review_text.replace("See more", "").strip()
        
    seller_name = listing.find(class_="review-footer-userNickname").text
    review_title = listing.find(class_="review-title").text
    star_rating = listing.find(class_="average-rating").text
    star_rating = float(re.sub('[()]', '', star_rating))
    
    helpful_upvotes = re.search(r'\((.*?)\)', 
                                listing.find(class_='yes-vote').text).group(1)
    unhelpful_upvotes = re.search(r'\((.*?)\)', 
                                  listing.find(class_='no-vote').text).group(1)
    date = listing.find(class_='review-date-submissionTime').text

    a_dict = {}    
    for variable in ["review_text", "author", "star_rating", "condition",
                     "attributes", "verified_review", "seller_name", 
                     "helpful_unhelpful", "date"]:
        a_dict[variable] = eval(variable)
    
    print(a_dict)