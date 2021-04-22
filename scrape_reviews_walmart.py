# -*- coding: utf-8 -*-
"""
Created on Tue Apr 20 16:27:19 2021

@author: Admin
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
    
def get_reviews_walmart(page_url):
    for page in range(1, 4):        
        page_url = page_url[0:-1] + str(page)
        response = requests.get(page_url)
        html = response.text
        
        soup = BeautifulSoup(html, "html5lib")
        
        review_sections = soup.select(".Grid.ReviewList-content")
        
        #Listings document
        for i, listing in enumerate(review_sections):
            try:
                review_text = listing.find(class_="review-text").text
                review_text = review_text.replace("See more", "").strip()
            except:
                pass
            
            try: 
                review_title = listing.find(class_="review-title").text
            except:
                pass
                
            author = listing.find(class_="review-footer-userNickname").text            
            star_rating = listing.find(class_="average-rating").text
            star_rating = float(re.sub('[()]', '', star_rating))
            source = "walmart"
                        
            helpful_upvotes = re.search(r'\((.*?)\)', 
                                        listing.find(class_='yes-vote').text).group(1)
            unhelpful_upvotes = re.search(r'\((.*?)\)', 
                                          listing.find(class_='no-vote').text).group(1)
            
            date_created = listing.find(class_='review-date-submissionTime').text
            datetime_scraped = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
            
            review_id = author + str(star_rating) + str(datetime_scraped)
            review_id = review_id.strip()
            
            if 'digital' in soup.find(class_="prod-ProductTitle").text.lower():
                model = 'Sony PlayStation 5 Digital Edition'
            else:
                model = 'Sony PlayStation 5 Disc Edition'
            
            review_params = {}    
            for variable in ["review_title", "review_text", "author", "star_rating",
                             "verified_review", "helpful_upvotes", "unhelpful_upvotes", 
                             "model", "date_created", "datetime_scraped", "source",
                             "review_id"]:
                review_params[variable] = eval(variable)
                
            review_params = json.loads(json.dumps(review_params), 
                                       parse_float=Decimal)
                
            yield(review_params)
            
            time.sleep(1)

#Disk Edition
url = "https://www.walmart.com/reviews/product/363472942?page=1"
for i in get_reviews_walmart(url):
    print(i)
    
#Digital Edition
url = "https://www.walmart.com/reviews/product/493824815?page=1"
for i in get_reviews_walmart(url):
    print(i)