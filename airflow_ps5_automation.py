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
	Create rest tables in postgress database
	"""
	#Get the postgress credentials from the airflow connection, establish connection 
	#and initiate cursor
	dynamodb = boto3.resource(kwargs['db'],
	                          aws_access_key_id=kwargs['key'],
	                          aws_secret_access_key= kwargs['s_key'],
	                          region_name=kwargs['r_name'])

    #Text for query to create tables
    #Drop each table before creating, as we want data from previous runs in this 
    #case to be erased
    #associate the tables with references to other tables
    #set primary keys
	
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
	def get_product_features(link):
	    response = requests.get(link)
	    html = response.text
	    soup = BeautifulSoup(html, "html5lib")
	    
	    item_id = (link.split("hash=item"))[1].split(":g:")[0]
	    try:
	        seller_name = soup.find(class_="mbg-nw").text
	    except:
	        print('Seller name not found', link)
	    
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

	    if attr_table is None:
	        dict_features = {}
	    else:
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

	dynamodb = boto3.resource(kwargs['db'],
	                          aws_access_key_id=kwargs['key'],
	                          aws_secret_access_key= kwargs['s_key'],
	                          region_name=kwargs['r_name'])

	def create_ebay_listings_PS5(db):
	    table = db.Table('ebay_listings_PS5') 
	    
	    url = "https://www.ebay.co.uk/sch/i.html?_from=R40&_nkw=ps5+console&_sacat=0&LH_TitleDesc=0&_pgn=1"
	        
	    for item in get_products(url):
	        table.put_item(Item=item)

	create_ebay_listings_PS5(dynamodb)


def create_reviews_dynamo(**kwargs):
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
	            review_id = review_id.strip()
	            
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
	                if variable in locals():
	                	review_params[variable] = eval(variable)
	                
	            review_params = json.loads(json.dumps(review_params), 
	                                       parse_float=Decimal)
	                
	            yield(review_params)
	            
	            time.sleep(1)

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
	            verified_review = 'yes'
	                        
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
	#Function for entering reviews in database
	def create_reviews_PS5(function, db):
	    table = db.Table('consumer_reviews_PS5') 
	        
	    for item in function:
	        table.put_item(Item=item)

	dynamodb = boto3.resource(kwargs['db'],
	                          aws_access_key_id=kwargs['key'],
	                          aws_secret_access_key= kwargs['s_key'],
	                          region_name=kwargs['r_name'])

	#Disk Edition Walmart
	url = "https://www.walmart.com/reviews/product/363472942?page=1"
	function =  get_reviews_walmart(url)

	create_reviews_PS5(function, dynamodb)

	    
	#Digital Edition Walmart
	url = "https://www.walmart.com/reviews/product/493824815?page=1"
	function =  get_reviews_walmart(url)

	create_reviews_PS5(function, dynamodb)

	#Digital Edition Ebay
	url = "https://www.ebay.co.uk/urw/Sony-PS5-Digital-Edition-Console-White/product-reviews/25040975636?pgn=1&condition=all"
	function =  get_reviews_ebay(url)

	create_reviews_PS5(function, dynamodb)


	#Disk edition Ebay
	url = "https://www.ebay.co.uk/urw/Sony-PS5-Blu-Ray-Edition-Console-White/product-reviews/19040936896?pgn=1&condition=all"
	function =  get_reviews_ebay(url)

	create_reviews_PS5(function, dynamodb)

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