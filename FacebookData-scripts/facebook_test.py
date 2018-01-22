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

logFile = '/home/user/Desktop/AutomationPrasanjit/FacebookData-scripts/facebook.log'
file=open(logFile, "a+")

def get_page_data(page_id,access_token,required_date):
    
    #api_endpoint = "https://graph.facebook.com/v2.4/" 			#correct
    
    # fb_graph_url = api_endpoint+page_id+"?fields=id,name,posts.since(2016-10-30){shares,full_picture,picture,message},talking_about_count,picture&access_token="+access_token
    #correct
    #fb_graph_url = api_endpoint+page_id+"?fields=id,name,posts{likes,shares,full_picture,picture,message,created_time},talking_about_count,picture,fan_count&access_token="+access_token#+"&since="+required_date+"&until="+required_date
    
    #fb_graph_url = api_endpoint+page_id+"?fields=id,name,posts.until("+1481049000.0+").since("+1480962600.0+"){likes,shares,full_picture,picture,message,created_time},talking_about_count,picture,
    #fan_count&access_token="+access_token#+"&since="+required_date+"&until="+required_date

    #the below url is working properly
    # fb_graph_url = api_endpoint+page_id+"?fields=id,name,picture&access_token="+access_token
    
    fb_graph_url = "https://graph.facebook.com/16336837009/insights/page_fans_country/lifetime?&since=2017-02-06&until=2017-02-07&access_token=347243862294545|a0ee0c600ae638b9cbc2ce170e95372d"
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

def getFacebookPageDetails(required_date):
	mydb = MySQLdb.connect(host='172.16.11.19', user='prasanjit', passwd='prasan123', db='carwaledata')
	cursor = mydb.cursor()

	cursor.execute("""SELECT PageNumber,CategoryId,CategoryTypeId 
    					FROM carwaleprotest.facebookpages order by PageNumber""")
	pageIds = cursor.fetchall();
	token = "347243862294545|a0ee0c600ae638b9cbc2ce170e95372d"  # Access Token

	for pageId in pageIds:
		pageData = get_page_data(str(pageId[0]),token,required_date)
		
		sql = """insert into Social_FacebookPages values(%s,%s,%s,%s,%s)"""
		args = (str(pageId[0]),pageId[1],pageData["name"].encode('utf-8'),pageId[2],pageData["picture"]["data"]["url"])
		try:
			cursor.execute(sql,args)
			mydb.commit()
		except Exception as ex:
			print str(ex)
			file.write(str(ex)+"\n")

def getFacebookPageDailyData(required_date):
	# mydb = MySQLdb.connect(host='172.16.11.19', user='prasanjit', passwd='prasan123', db='carwaledata')
	# cursor = mydb.cursor()

	# cursor.execute("""SELECT PageId
 #    					FROM carwaledata.Social_FacebookPages order by PageId""")
	# pageIds = cursor.fetchall();
	token = "347243862294545|a0ee0c600ae638b9cbc2ce170e95372d"  # Access Token

	# for pageId in pageIds:
	# 	pageData = get_page_data(str(pageId[0]),token,required_date)
	# 	postCount = 0
	# 	# post_count = len(pageData["posts"]["data"]) ####this is the total post count , 
	# 												###but we require per day post count
	# 	if pageData:
	# 		for data in pageData["posts"]["data"]:
	# 			post_id = data["id"]
	# 			if(required_date in data["created_time"]):
	# 				postCount = postCount+1

	# 			if('message' in data):
	# 				message = str(data["message"].encode('ascii', 'ignore'))
	# 			if('likes' in data):
	# 				LikesCount = len(data["likes"]["data"])
	# 			if('shares' in data):
	# 				sharesCount = data["shares"]["count"]
	# 			if('picture' in data):
	# 				picture = data["picture"]
	# 			if('full_picture' in data):
	# 				full_picture = data["full_picture"]

	# 			cursor.execute("""SELECT ID from Social_FacebookPages_daily_Posts 
	# 									WHERE ID = %s""",post_id)
	# 			check_post_id = cursor.fetchall();

	# 			# print not check_post_id
	# 			if check_post_id:
	# 				sql2 = """UPDATE Social_FacebookPages_daily_Posts 
	# 				SET Message = %s,LikesCount = %s,ShareCount = %s WHERE ID = %s"""
	# 				args2 = (message,LikesCount,sharesCount,post_id)
	# 				# print "updating the existing posts"
	# 			else:
	# 				sql2 = """insert into Social_FacebookPages_daily_Posts values(%s,%s,%s,%s,%s,%s,%s,%s)"""
	# 				args2 = (post_id,data["created_time"][:10],pageId[0],message,LikesCount,sharesCount,picture,full_picture)
	# 				# print "inserting new posts"
				
	# 			try:
	# 				cursor.execute(sql2,args2)
	# 				mydb.commit()
	# 			except Exception as ex:
	# 				print str(ex)
	# 				file.write(str(ex)+"\n")
	# 				break
					
	# 		sql1 = """insert into Social_FacebookPages_daily values(%s,%s,%s,%s,%s)"""
	# 		args1 = (pageId[0],required_date,pageData["fan_count"],postCount,pageData["talking_about_count"])
			
	# 		try:
	# 			cursor.execute(sql1,args1)
	# 			mydb.commit()
	# 		except Exception as ex:
	# 			print str(ex)
	# 			file.write(str(ex)+"\n")
	# 			break

	pageData = get_page_data("",token,required_date)
	data = pageData["data"][0]["values"][0]["value"]
	LikesCount = 0
	for item in data:
		key = str(item[0:2])
		LikesCount = LikesCount + int(data[key])

	print "LikesCount : "+str(LikesCount)

def main():
	token = "347243862294545|a0ee0c600ae638b9cbc2ce170e95372d"  # Access Token
	today = datetime.date.today()
	required_date = (today + timedelta(days=-1)).strftime('%Y-%m-%d')

	file.write(required_date+" : \n")
	# getFacebookPageDetails(required_date)
	getFacebookPageDailyData(required_date)
	file.write("Successfuly Fetched FacebookData \n")
if __name__ =='__main__':
    main()