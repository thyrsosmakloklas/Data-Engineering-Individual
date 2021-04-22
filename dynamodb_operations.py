# -*- coding: utf-8 -*-
"""
Created on Thu Apr 22 01:49:01 2021

@author: Admin
"""
import boto3

#Create Table for ebay listings
table = dynamodb.create_table(
TableName = 'ebay_listings_PS5',
KeySchema = [
        {
            "AttributeName": "item_id",
            "KeyType": "HASH"
        }
    ],
    AttributeDefinitions = [
        {
            "AttributeName": "item_id",
            "AttributeType": 'S'
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
    }
)

def create_ebay_listings_PS5():
    table = dynamodb.Table('ebay_listings_PS5') 
    
    url = "https://www.ebay.co.uk/sch/i.html?_from=R40&_nkw=ps5+console&_sacat=0&LH_TitleDesc=0&_pgn=1"
        
    for item in get_products(url):
        table.put_item(Item=item)

create_ebay_listings_PS5()

#table.delete()

#Create Table for ebay & walmart reviews
table = dynamodb.create_table(
TableName = 'consumer_reviews_PS5',
KeySchema = [
        {
            "AttributeName": "review_id",
            "KeyType": "HASH"
        }
    ],
    AttributeDefinitions = [
        {
            "AttributeName": "review_id",
            "AttributeType": 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
    }
)

#Function for entering reviews in database
def create_reviews_PS5(function):
    table = dynamodb.Table('consumer_reviews_PS5') 
        
    for item in function:
        table.put_item(Item=item)

#Disk Edition Walmart
url = "https://www.walmart.com/reviews/product/363472942?page=1"
function =  get_reviews_walmart(url)

create_reviews_PS5(function)

    
#Digital Edition Walmart
url = "https://www.walmart.com/reviews/product/493824815?page=1"
function =  get_reviews_walmart(url)

create_reviews_PS5(function)

#Digital Edition Ebay
url = "https://www.ebay.co.uk/urw/Sony-PS5-Digital-Edition-Console-White/product-reviews/25040975636?pgn=1&condition=all"
function =  get_reviews_ebay(url)

create_reviews_PS5(function)


#Disk edition Ebay
url = "https://www.ebay.co.uk/urw/Sony-PS5-Blu-Ray-Edition-Console-White/product-reviews/19040936896?pgn=1&condition=all"
function =  get_reviews_ebay(url)

create_reviews_PS5(function)
    
    