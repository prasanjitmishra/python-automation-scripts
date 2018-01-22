#!/usr/bin/python
import sys
import string
import simplejson
from twython import Twython
import datetime
import time
from datetime import timedelta
import MySQLdb
import json
import codecs
from twython import TwythonStreamer

#twitter Keys
keys = Twython(app_key='l8Ze1aX0FVi9QOg9L907vbjSm', #REPLACE 'APP_KEY' WITH YOUR APP KEY, ETC., IN THE NEXT 4 LINES
    app_secret='4tRVSw4iJQlKi5asn7X0cyJ5j0hPVYrHu6vi0qbwTUIs5AWwVb',
    oauth_token='1261716252-FfKpywNDJWeEzGYZfreg7NJtea2hpabnpVd48Ze',
    oauth_token_secret='lj43IrSSY5RtT53wGYmBaAh9EL0kzhdAO03mE9XqOZn7v')

#Set Date
today = datetime.date.today()
required_date = (today + timedelta(days=-1)).strftime('%Y-%m-%d')

#Set Database
DBhost='cwprodb.cpur6ljmwsc0.ap-southeast-1.rds.amazonaws.com'
DBuser='cwpro'
DBpasswd='carwalepro'
DB='carwalepro'

# TWITTER_CONSUMER_KEY='l3l9MlL7o1CeO9GYRGwGD5DKV'
# TWITTER_CONSUMER_SECRET='cinoFeSCdsK1ilBsBZ3cRNRZUlihpRBHVxx96WY33AWt4nQQnQ'
# TWITTER_ACCESS_TOKEN='1261716252-7WicbBRe7uzfJ0Tkq6VWPFKxr4fy0Qdkk3CnRK7'
# TWITTER_ACCESS_TOKEN_SECRET='Nkj2H3pghqI37QJJwS7eyTh5qgKTwEEMtIOnADLiHBjYI'

#set Log File
logFile = '/home/ubuntu/carwalepro/Social_Scripts/social.log'
file=open(logFile, "a+")

def convertMonth(month):
	if month == 'Jan':
		month = '01' 
	elif month == 'Feb':
		month = '02' 
	elif month == 'Mar':
		month = '03' 
	elif month == 'Apr':
		month = '04' 
	elif month == 'May':
		month = '05' 
	elif month == 'Jun':
		month = '06' 
	elif month == 'Jul':
		month = '07' 
	elif month == 'Aug':
		month = '08' 
	elif month == 'Sep':
		month = '09' 
	elif month == 'Oct':
		month = '10' 
	elif month == 'Nov':
		month = '11' 
	elif month == 'Dec':
		month = '12'
	
	return  month

def get_daily_twitter_page_details():
	mydb = MySQLdb.connect(host=DBhost, user=DBuser, passwd=DBpasswd, db=DB)
	cursor = mydb.cursor()

	cursor.execute("""SELECT ID,PageId,Name
    					FROM Social_TwitterPages order by ID asc""")
	Pageids = cursor.fetchall();
	
	for Pageid in Pageids:
		try:
			users = keys.lookup_user(user_id = Pageid[1])
		except Exception,e:
			file.write(str(e))
			# print str(e)
			break
		
		followersCount = users[0]["followers_count"]
		listed_count = users[0]["listed_count"]
		statuses_count = users[0]["statuses_count"]
		friends_count = users[0]["friends_count"]
		favourites_count = users[0]["favourites_count"]

		args = (Pageid[0],required_date)
		cursor.execute("""SELECT PageId,Date
    		FROM Social_twitterPages_daily WHERE PageId = %s and Date = %s""",args)
		data = cursor.fetchall()

		if not data:
			sql = """insert into Social_twitterPages_daily values(%s,%s,%s,%s,%s,%s,%s)"""
			args = [Pageid[0],required_date,followersCount,listed_count,statuses_count,friends_count,favourites_count]
			cursor.execute(sql,args)
			mydb.commit()
	
	cursor.close()
	mydb.close()

def main():
	file.write("Started Fetching Twitter Data for : "+required_date+"\n")
	get_daily_twitter_page_details()
	
	file.write("Twitter data updation Completed")
if __name__ =='__main__':
    main()