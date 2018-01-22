#!/usr/bin/python
import urllib2
import json
import MySQLdb
import subprocess
import io
import codecs
import datetime
import time
from datetime import timedelta

logFile = '/home/ubuntu/carwalepro/Social_Scripts/social.log'
file=open(logFile, "a+")

access_token = "347243862294545|a0ee0c600ae638b9cbc2ce170e95372d"  # Access Token

#Set Database
DBhost='cwprodb.cpur6ljmwsc0.ap-southeast-1.rds.amazonaws.com'
DBuser='cwpro'
DBpasswd='carwalepro'
DB='carwalepro'

today = datetime.date.today()
required_date = (today + timedelta(days=-1)).strftime('%Y-%m-%d')

def get_page_data(page_id):
    api_endpoint = "https://graph.facebook.com/v2.4/"
    # fb_graph_url = api_endpoint+page_id+"?fields=id,name,posts.since(2016-10-30){shares,full_picture,picture,message},talking_about_count,picture&access_token="+access_token
    fb_graph_url = api_endpoint+page_id+"?fields=id,name,posts{likes,shares,full_picture,picture,message,created_time},talking_about_count,picture,fan_count&access_token="+access_token#+"&since="+required_date+"&until="+required_date
    
    #fb_graph_url = api_endpoint+page_id+"?fields=id,name,posts.until("+1481049000.0+").since("+1480962600.0+"){likes,shares,full_picture,picture,message,created_time},talking_about_count,picture,
    #fan_count&access_token="+access_token#+"&since="+required_date+"&until="+required_date

    #the below url is working properly
    # fb_graph_url = api_endpoint+page_id+"?fields=id,name,picture&access_token="+access_token
    
    try:
        api_request = urllib2.Request(fb_graph_url)
        api_response = urllib2.urlopen(api_request)
        
        try:
            return json.loads(api_response.read())
        except (ValueError, KeyError, TypeError):
            file.write("JSON error \n")
            return "JSON error"
            
    except IOError, e:
        if hasattr(e, 'code'):
            file.write(str(e.code)+"\n")
            return e.code
        elif hasattr(e, 'reason'):
        	file.write(str(e.reason)+"\n")
        	return e.reason

def getFacebookPageDailyData():
	mydb = MySQLdb.connect(host=DBhost, user=DBuser, passwd=DBpasswd, db=DB)
	cursor = mydb.cursor()

	cursor.execute("""SELECT ID,PageId
    					FROM Social_FacebookPages order by PageId""")
	pageIds = cursor.fetchall();

	for pageId in pageIds:
		pageData = get_page_data(str(pageId[1]))
		postCount = 0
		# post_count = len(pageData["posts"]["data"]) ####this is the total post count , 
													###but we require per day post count
		if pageData:
			for data in pageData["posts"]["data"]:
				post_id = data["id"]
				if(required_date in data["created_time"]):
					postCount = postCount+1
					
			sql1 = """insert into Social_FacebookPages_Daily values(%s,%s,%s,%s,%s)"""
			args1 = (str(pageId[0]),required_date,pageData["fan_count"],postCount,pageData["talking_about_count"])
			
			try:
				cursor.execute(sql1,args1)
				mydb.commit()
			except Exception as ex:
				# print str(ex)
				file.write(str(ex)+"\n")
				break

def main():
	file.write("Started Fetching FacebookData for : "+required_date+"\n")
	getFacebookPageDailyData()
	file.write("Completed Fetching FacebookData \n")
if __name__ =='__main__':
    main()