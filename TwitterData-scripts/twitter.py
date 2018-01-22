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
DBhost='172.16.11.19'
DBuser='prasanjit'
DBpasswd='prasan123'
DB='carwaledata'

# TWITTER_CONSUMER_KEY='l3l9MlL7o1CeO9GYRGwGD5DKV'
# TWITTER_CONSUMER_SECRET='cinoFeSCdsK1ilBsBZ3cRNRZUlihpRBHVxx96WY33AWt4nQQnQ'
# TWITTER_ACCESS_TOKEN='1261716252-7WicbBRe7uzfJ0Tkq6VWPFKxr4fy0Qdkk3CnRK7'
# TWITTER_ACCESS_TOKEN_SECRET='Nkj2H3pghqI37QJJwS7eyTh5qgKTwEEMtIOnADLiHBjYI'

#set Log File
logFile = '/home/user/Desktop/AutomationPrasanjit/TwitterData-scripts/twitter_data.log'
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

	cursor.execute("""SELECT PageId,Name
    					FROM carwaledata.Social_TwitterPages order by PageId asc""")
	Pageids = cursor.fetchall();
	
	for Pageid in Pageids:
		try:
			users = keys.lookup_user(user_id = Pageid[0])
		except Exception,e:
			file.write(str(e))
			print str(e)
			break
		
		followersCount = users[0]["followers_count"]
		listed_count = users[0]["listed_count"]
		statuses_count = users[0]["statuses_count"]
		friends_count = users[0]["friends_count"]
		favourites_count = users[0]["favourites_count"]

		args = (Pageid[0],required_date)
		cursor.execute("""SELECT PageId,Date
    		FROM carwaledata.Social_twitterPages_daily WHERE PageId = %s and Date = %s""",args)
		data = cursor.fetchall()

		if not data:
			sql = """insert into Social_twitterPages_daily values(%s,%s,%s,%s,%s,%s,%s)"""
			args = [required_date,Pageid[0],followersCount,listed_count,statuses_count,friends_count,favourites_count]
			cursor.execute(sql,args)
			mydb.commit()
	
	cursor.close()
	mydb.close()

def get_daily_twitter_posts_details():
	mydb = MySQLdb.connect(host=DBhost, user=DBuser, passwd=DBpasswd, db=DB)
	cursor = mydb.cursor()

	cursor.execute("""SELECT PageId,Name
    					FROM carwaledata.Social_TwitterPages order by PageId asc""")
	Pageids = cursor.fetchall()
	
	for Pageid in Pageids:
		try:
			tweets = keys.get_user_timeline(screen_name=Pageid[1],count="200",page="1",include_entities="true",include_rts="1")
			
			date = tweets[0]["created_at"].split(" ")
			date = date[5]+"-"+convertMonth(date[1])+"-"+date[2]
			
			for tweet in tweets:
				if len(tweet["entities"]["urls"]) != 0:				
					url = tweet["entities"]["urls"][0]["url"]
				else:
					url = None

				tweetId = tweet["id"]
				message = tweets[0]["text"].encode('ascii', 'ignore')
				favoriteCount = tweet["favorite_count"]
				retweetCount = tweet["retweet_count"]

				cursor.execute("""SELECT Id
    					FROM carwaledata.Social_Twitter_daily_Posts where Id = %s""",tweetId)
				postId = cursor.fetchall();
				# if not postId:
				# 	print "new"
				# 	postId = tweetId
				# else:
				# 	print "old"
				# 	postId = postId[0][0]
				# print postId

				if not postId:
					sql = """insert into Social_Twitter_daily_Posts values(%s,%s,%s,%s,%s,%s,%s)"""
					args = (tweetId,date,Pageid[0],message,favoriteCount,retweetCount,url)
					print "insert"
				else:
					sql = """UPDATE Social_Twitter_daily_Posts 
					SET Message = %s,FavouriteCount = %s,RetweetCount = %s WHERE Id = %s"""
					args = (message,favoriteCount,retweetCount,postId[0][0])
					print "update"
				try:
					cursor.execute(sql,args)
					mydb.commit()
				except Exception as ex:
					print str(ex)
					file.write(str(ex)+"\n")

		except Exception,e:
			print str(e)
			file.write(str(e))
			break
	
	cursor.close()
	mydb.close()

def main():
	file.write("\n"+str(required_date)+":")
	get_daily_twitter_page_details()
	get_daily_twitter_posts_details()
	
	file.write("Twitter data updated successfully")
if __name__ =='__main__':
    main()