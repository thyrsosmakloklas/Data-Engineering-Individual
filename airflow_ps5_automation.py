from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
import logging

import boto3
import requests
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime
from decimal import Decimal
import json
import pandas as pd

log = logging.getLogger(__name__)

# =============================================================================
# 1. Set up the main configurations of the dag
# =============================================================================


default_args = {
    'start_date': datetime(2021, 4, 20),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'bucket_name': 'ucl-msin0166-2021-individual-tmakloklas',
    'prefix': 'test_folder',
    'db': Variable.get("dynamo_ps5_secret", deserialize_json=True)['db'],
    'key': Variable.get("dynamo_ps5_secret", deserialize_json=True)['key'],
    's_key': Variable.get("dynamo_ps5_secret", deserialize_json=True)['s_key'],
    'r_name': Variable.get("dynamo_ps5_secret", deserialize_json=True)['r_name'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('ps5_project',
    description='ps5_project',
    schedule_interval='@weekly',
    catchup=False,
    default_args=default_args,
    max_active_runs=1)

# =============================================================================
# 2. Define different functions
# =============================================================================
def create_tables_dynamo(**kwargs):
	"""
	Create tables in dynamoDB database
	"""
	#Get the dynamoDB credentials from the airflow airflow variabe, establish connection 
	dynamodb = boto3.resource(kwargs['db'],
	                          aws_access_key_id=kwargs['key'],
	                          aws_secret_access_key= kwargs['s_key'],
	                          region_name=kwargs['r_name'])

	#Create dynamoDB tables
	#Set key, type of key and capacity units	
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
	        }
	    ],
	    ProvisionedThroughput={
	        'ReadCapacityUnits': 1,
	        'WriteCapacityUnits': 1
	    }
	)

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

	log.info("Created Tables")

def create_ebay_listings_dynamo(**kwargs):
	#Ebay listings
	def get_product_features(link):
	    """ 
	    Input: Product Link
	    Output: Dictionary of faeatures, raw and processed

	    Gets the product features data of a product page
	    """
	    #Get html and create beautiful soup object
	    response = requests.get(link)
	    html = response.text
	    soup = BeautifulSoup(html, "html5lib")
	    
	    #Get features
	    #Item_id = text between two strings
	    item_id = (link.split("hash=item"))[1].split(":g:")[0]

	    #If seller name not found, print the link
	    try:
	        #Get seller name
	        seller_name = soup.find(class_="mbg-nw").text
	    except:
	        print('Seller name not found', link)
	    
	    #If not found pass
	    try:
	    	#Get item split on % and get the first item in list as float
	        seller_pos_feedback_12m_per_raw = soup.find(id="si-fb").text
	        seller_pos_feedback_12m_per = float(seller_pos_feedback_12m_per_raw.split("%")[0])
	    except:
	        pass
	    
	    #If not found pass
	    try:
	    	#Get item split on \t and get the last item as float
	        product_rating_avg_raw = soup.find(class_="ebay-review-start-rating").text
	        product_rating_avg = float(product_rating_avg_raw.split("\t")[-1])
	    except:
	        pass
	        
	    #Get the table object
	    attr_table = soup.find(class_="itemAttr")
	    attributes = []

	    #if the table object does not exist initiate an empty dict
	    if attr_table is None:
	        dict_features = {}
	    else:
	    	#Else for every attribute label
	        for el in attr_table.find_all(class_="attrLabels"):
	        	#get the name of the attribute clean of spaces and : in lower
	            attributes.append(el.text.strip().replace(":", "").lower())

	            #Get the number of attributes
	            len_attrs = len(attributes)

	            dict_features = {}
	            #for every attribute get its span
	            for i, el in enumerate(list(attr_table.find_all("span"))[-len_attrs:]):
	                try:
	                    dict_features[attributes[i]] = el.text
	                except:
	                    pass
	            #if condition is in the attributes remove it as we'll have it twice
	            if 'condition' in map(str.lower,attributes):
	                try:
	                    del dict_features['condition']
	                except:
	                    del dict_features['Condition']
	    
	    #Iterate over all the variables created and add them to the dictionary for 
	    #raw and processed data
	    features_raw = {'features': dict_features}
	    for variable in ["item_id", "seller_name", "seller_pos_feedback_12m_per_raw",
	                     "product_rating_avg_raw"]:
	        try:
	            features_raw[variable] = eval(variable)
	        except:
	            pass

	    features = {'features': dict_features}
	    for variable in ["item_id", "seller_name", "seller_pos_feedback_12m_per",
	                     "product_rating_avg"]:
	        try:
	            features[variable] = eval(variable)
	        except:
	            pass      
	    
	    #return both raw and processed data
	    return(features_raw, features)

	def get_products(page_url):
	    """ 
        Input: Page url
        Output: Yield of dictionaries

        Iterable object that yields a dicitonary of data for each eBay listing
	    """
	    #for 30 pages of listings
	    for page in range(1, 31):
	        #iterate over urls, get html and create beautifulsoup object
	        page_url = page_url[0:-1] + str(page)
	        url = page_url
	        response = requests.get(url)
	        html = response.text
	        
	        soup = BeautifulSoup(html, "html5lib")
	        
	        #for every listing in the page
	        for listing in soup.find_all(class_=("s-item--watch-at-corner")):
	            
	            #get title and remove the new listing if it exists
	            listing_title_raw = listing.find("h3", class_=("s-item__title")).text
	            listing_title = listing_title_raw.strip().replace("New listing", "")
	            
	            #if 5 is not mentioned in the title, disregard, we are after PS5
	            if str(5) not in listing_title:
	                pass
	            
	            #Get price, without pounds, for other currencies return the raw price
	            price_raw = listing.find(class_=("s-item__detail s-item__detail--primary")).text
	            try:
	                price = re.split("£|x", price_raw)[1]
	            except:
	                price = price_raw
	            
	            #gather data
	            photo_url = listing.find(class_="s-item__image-img")["src"]
	            condition = listing.find(class_="SECONDARY_INFO").text
	            item_link = listing.find(class_="s-item__link")["href"]

	            #set the time it was scraped
	            datetime_scraped = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
	                        
	            #for every variable created add it to raw or processed dictionary            
	            listing_features_raw = {}
	            for variable in ["listing_title_raw", "price_raw", "photo_url", 
	                             "condition", "item_link"]:
	                listing_features_raw[variable] = eval(variable)
	                
	            listing_features = {}
	            for variable in ["listing_title", "price", "photo_url", 
	                             "condition", "item_link", 'datetime_scraped']:
	                listing_features[variable] = eval(variable)
	            
	            #get product features
	            product_features = get_product_features(item_link)
	            
	            #unpack both dicitonaries in one and turn it to json, with decimals in place of float
	            output_raw = {**listing_features_raw, **product_features[0]}
	            output_raw = json.loads(json.dumps(output_raw), parse_float=Decimal)

	            output = {**listing_features, **product_features[1]}
	            output = json.loads(json.dumps(output), parse_float=Decimal)
	            
	            time.sleep(1)
	            
	            #yield the output
	            yield(output_raw, output)

	def create_ebay_listings_PS5(db, s3, bucket_name):
	    """
	    Input: s3, db instance, bucket name
	    Output: None

	    Iterates over the get_products object, places raw and processed data in s3, inputs items
	    in dynamoDB
	    """
	    #set table name, url
	    table = db.Table('ebay_listings_PS5') 
	    
	    url = "https://www.ebay.co.uk/sch/i.html?_from=R40&_nkw=ps5+console&_sacat=0&LH_TitleDesc=0&_pgn=1"
	    
	    #create empty lists for data to move to s3    
	    data_raw = []
	    data = []
	    
	    #iterate over yield object
	    for item in get_products(url):
	        #0 for raw data, 1 for processed
	        #append them in lists
	        data_raw.append(item[0])
	            
	        data.append(item[1])
	        
	        #put processed items in db
	        table.put_item(Item=item[1])
	    
	    #Transform json to parquet
	    df_raw = pd.DataFrame(data_raw)
	    df_raw_parquet = df_raw.to_parquet()
	    
	    #Drop to s3
	    object = s3.Object(bucket_name, 'df_raw_listingsEbay.parquet')
	    object.put(Body=df_raw_parquet)
	    
	    #Transform json to parquet
	    df = pd.DataFrame(data)
	    df_parquet = df.to_parquet()
	    
	    #Drop to s3
	    object = s3.Object(bucket_name, 'df_listingsEbay.parquet')
	    object.put(Body=df_parquet)

	#initiate instances
	dynamodb = boto3.resource(kwargs['db'],
	                          aws_access_key_id=kwargs['key'],
	                          aws_secret_access_key= kwargs['s_key'],
	                          region_name=kwargs['r_name'])

	s3 = boto3.resource('s3',
	                    aws_access_key_id=kwargs['key'],
	                    aws_secret_access_key= kwargs['s_key'],
	                    region_name=kwargs['r_name'])	

	#set bucket name
	bucket_name = kwargs['bucket_name']

	create_ebay_listings_PS5(dynamodb, s3, bucket_name)


def create_reviews_dynamo(**kwargs):
	def get_reviews_ebay(page_url):
	    """
	    Input: Page url of ebay reviews
	    Output: yield of raw and processed diictionaries

	    Iterates over review pages of ebay reviews and gets info on each review
	    """
	    #Iterate over pages
	    for page in range(1, 51):        
	        page_url = page_url[:-15] + str(page)+ page_url[-14:]        
	        #Get html, turn to beautiful soup object
	        response = requests.get(page_url)
	        html = response.text
	        soup = BeautifulSoup(html, "html5lib")
	        
	        #Get review section
	        review_sections = soup.find_all(class_="ebay-review-section")
	        
	        time.sleep(1)

	        #Iterate over each review
	        for i, review in enumerate(review_sections):
	            #if there is text get and strip off spaces and perceiding text
	            try:
	                review_text_raw = review.find(class_="review-item-content").text
	                review_text = review_text_raw.replace("Read full review...", "").strip()
	            except:
	                pass
	            
	            #get striped author name
	            author_raw = review.find(class_="review-item-author").text
	            author = author_raw.strip()
	            
	            #Get star rating, split on space, select first item of list
	            star_rating_raw = review.find(class_="star-rating")["aria-label"]
	            star_rating = float(star_rating_raw.split(" ")[0])
	            
	            #get review title, strip spaces
	            review_title_raw = review.find(class_="review-item-title").text
	            review_title = review_title_raw.strip()
	            
	            #get attribute item
	            attributes = review.find_all(class_="rvw-val")
	            try:
	                #if its a verified review get all attributes
	                #otherwise they don't exist
	                verified_review_raw = attributes[0].text
	                verified_review = verified_review_raw.strip()
	                if verified_review.lower == 'yes':
	                    condition_raw = attributes[1].text
	                    condition = condition_raw.strip()
	                    
	                    seller_name_raw = attributes[2].text
	                    seller_name = seller_name_raw.strip()
	            except:
	                pass
	            
	            #set source
	            source = "ebay"
	            
	            #Get helpful and unhelpful upvotes to float
	            helpful_upvotes_raw = review.find(class_='positive-h-c').text
	            helpful_upvotes = float(helpful_upvotes_raw)
	            
	            unhelpful_upvotes_raw = review.find(class_='negative-h-c').text
	            unhelpful_upvotes = float(unhelpful_upvotes_raw)
	            
	            #Get date created, transform to datatime with format %Y-%m-%d
	            date_created_raw = review.find(class_='review-item-date').text
	            date_created = datetime.strptime(date_created_raw,'%d %b, %Y').strftime('%Y-%m-%d')
	            
	            #create datetime scraped
	            datetime_scraped = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
	            
	            #create review id, strip
	            review_id = author + str(star_rating) + str(datetime_scraped)
	            review_id = review_id.strip()
	            
	            #if digital in page url set model to digital, else disc
	            if 'digital' in page_url.lower():
	                model = 'Sony PlayStation 5 Digital Edition'
	            else:
	                model = 'Sony PlayStation 5 Disc Edition'
	            
	            #for every variable created place it in dictionary, turn it to json with float as decimal
	            review_params_raw = {}
	            for variable in ["review_title_raw", "review_text_raw", "author_raw", "star_rating_raw", 
	                             "condition_raw", "verified_review_raw", "seller_name_raw", 
	                             "helpful_upvotes_raw", "unhelpful_upvotes_raw", "date_created_raw"]:
	                if variable in locals():
	                	review_params_raw[variable] = eval(variable)
	                
	            review_params_raw = json.loads(json.dumps(review_params_raw), 
	                                       parse_float=Decimal)
	            
	            review_params = {}
	            for variable in ["review_title", "review_text", "author", "star_rating", 
	                             "condition", "verified_review", "seller_name", 
	                             "helpful_upvotes", "unhelpful_upvotes", "model",
	                             "date_created", "datetime_scraped", "source", "review_id"]:
	                if variable in locals():
	                	review_params[variable] = eval(variable)
	                
	            review_params = json.loads(json.dumps(review_params), 
	                                         parse_float=Decimal)            
	                
	            #yield output
	            yield(review_params_raw, review_params)

	#Walmart reviews
	def get_reviews_walmart(page_url):
	    """
	    Input: Page url of ebay reviews
	    Output: yield of raw and processed diictionaries

	    Iterates over review pages of walmart reviews and yields info on each review
	    """
	    #Iterate over each page
	    for page in range(1, 51):        
	        page_url = page_url[0:-1] + str(page)
	        #Get html, turn to beautiful soup object
	        response = requests.get(page_url)
	        html = response.text
	        soup = BeautifulSoup(html, "html5lib")
	        
	        #Select the reviews section
	        review_sections = soup.select(".Grid.ReviewList-content")
	        
	        #sleep one second between pages
	        time.sleep(1)

	        #Iterate over reviews
	        for i, review in enumerate(review_sections):
	            try:
	            	#Get text, strip off space and perceiding text if exists
	                review_text_raw = review.find(class_="review-text").text
	                review_text = review_text_raw.replace("See more", "").strip()
	            except:
	                pass
	            
	            try: 
	                #if title exists, get
	                review_title = review.find(class_="review-title").text
	            except:
	                pass
	                
	            #Get author
	            author = review.find(class_="review-footer-userNickname").text 
	            
	            #Get star rating remove parenthesis
	            star_rating_raw = review.find(class_="average-rating").text
	            star_rating = float(re.sub('[()]', '', star_rating_raw))
	            
	            #Set source and verified review
	            #all reviews on walmart are verified
	            source = "walmart"
	            verified_review = 'yes'
	            
	            #Get helpful, unhelpful upvotes
	            helpful_upvotes_raw = review.find(class_='yes-vote').text
	            unhelpful_upvotes_raw = review.find(class_='no-vote').text
	            
	            #Keep numbers
	            helpful_upvotes = re.search(r'\((.*?)\)', helpful_upvotes_raw).group(1)
	            unhelpful_upvotes = re.search(r'\((.*?)\)', unhelpful_upvotes_raw).group(1)
	            
	            #Get created date, to format %Y-%m-%d
	            date_created_raw = review.find(class_='review-date-submissionTime').text
	            date_created = datetime.strptime(date_created_raw,'%B %d, %Y').strftime('%Y-%m-%d')
	            
	            #create datetime scraped
	            datetime_scraped = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
	            
	            #Create review_id
	            review_id = author + str(star_rating) + str(datetime_scraped)
	            review_id = review_id.strip()
	            
	            #If digital in page title = digital, else disk
	            if 'digital' in soup.find(class_="prod-ProductTitle").text.lower():
	                model = 'Sony PlayStation 5 Digital Edition'
	            else:
	                model = 'Sony PlayStation 5 Disc Edition'
	            
	            #for every variable created place it in either processed or raw dictionaries
	            #transform to json with float as decimals
	            review_params_raw = {}    
	            for variable in ["review_title", "review_text_raw", "author", "star_rating_raw",
	                             "helpful_upvotes_raw", "unhelpful_upvotes_raw", "date_created_raw",]:
	                review_params_raw[variable] = eval(variable)
	                
	            review_params_raw = json.loads(json.dumps(review_params_raw), 
	                                           parse_float=Decimal)

	            review_params = {}    
	            for variable in ["review_title", "review_text", "author", "star_rating",
	                             "verified_review", "helpful_upvotes", "unhelpful_upvotes", 
	                             "model", "date_created", "datetime_scraped", "source",
	                             "review_id"]:
	                review_params[variable] = eval(variable)
	                
	            review_params = json.loads(json.dumps(review_params), 
	                                         parse_float=Decimal)            
	                
	            yield(review_params_raw, review_params)

	#Function for entering reviews in database
	def create_reviews_PS5(function, db, s3, bucket_name, filename):
	    """
	    Input: function, s3, db instance, bucket name, filename
	    Output: None

	    Iterates over the get_reviews objects, places raw and processed data in s3, inputs items
	    in dynamoDB
	    """
	    #set table
	    table = db.Table('consumer_reviews_PS5') 
	    data_raw = []
	    data = []

	    #for each item place in lists and in dynamoDB
	    for item in function:
	        data_raw.append(item[0])
	            
	        data.append(item[1])
	        
	        table.put_item(Item=item[1])
	    
	    #Transform json to parquet
	    df_raw = pd.DataFrame(data_raw)
	    df_raw_parquet = df_raw.to_parquet()
	    
	    #Drop to s3
	    object = s3.Object(bucket_name, 'df_raw_' + filename +'.parquet')
	    object.put(Body=df_raw_parquet)
	    
	    #Transform json to parquet
	    df = pd.DataFrame(data)
	    df_parquet = df.to_parquet()
	    
	    #Drop to s3
	    object = s3.Object(bucket_name, 'df_' + filename +'.parquet')
	    object.put(Body=df_parquet)

	#Create instances
	dynamodb = boto3.resource(kwargs['db'],
	                          aws_access_key_id=kwargs['key'],
	                          aws_secret_access_key= kwargs['s_key'],
	                          region_name=kwargs['r_name'])

	s3 = boto3.resource('s3',
	                    aws_access_key_id=kwargs['key'],
	                    aws_secret_access_key= kwargs['s_key'],
	                    region_name=kwargs['r_name'])	

	bucket_name = kwargs['bucket_name']

	#Disk Edition Walmart
	url = "https://www.walmart.com/reviews/product/363472942?page=1"
	function =  get_reviews_walmart(url)

	create_reviews_PS5(function, dynamodb, s3, bucket_name, 'walmart_disk_reviews')
	    
	#Digital Edition Walmart
	url = "https://www.walmart.com/reviews/product/493824815?page=1"
	function =  get_reviews_walmart(url)

	create_reviews_PS5(function, dynamodb, s3, bucket_name, 'walmart_digital_reviews')

	#Digital Edition Ebay
	url = "https://www.ebay.co.uk/urw/Sony-PS5-Digital-Edition-Console-White/product-reviews/25040975636?pgn=1&condition=all"
	function =  get_reviews_ebay(url)

	create_reviews_PS5(function, dynamodb, s3, bucket_name, 'ebay_digital_reviews')

	#Disk edition Ebay
	url = "https://www.ebay.co.uk/urw/Sony-PS5-Blu-Ray-Edition-Console-White/product-reviews/19040936896?pgn=1&condition=all"
	function =  get_reviews_ebay(url)

	create_reviews_PS5(function, dynamodb, s3, bucket_name, 'ebay_disk_reviews')

# =============================================================================
# 3. Set up the dags
# =============================================================================
create_dynamo_tables = PythonOperator(
    task_id='create_tables_dynamo',
    python_callable=create_tables_dynamo,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

write_listings_dynamo = PythonOperator(
    task_id='create_ebay_listings_dynamo',
    python_callable=create_ebay_listings_dynamo,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

write_reviews_dynamo = PythonOperator(
    task_id='create_reviews_dynamo',
    python_callable=create_reviews_dynamo,
    op_kwargs=default_args,
    provide_context=True,
    dag=dag,
)

# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================
create_dynamo_tables >> write_listings_dynamo >> write_reviews_dynamo