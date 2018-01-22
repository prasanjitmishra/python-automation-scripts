import pyodbc
import csv
import subprocess
import MySQLdb
csvfile = '/home/user/Desktop/pricequoteOctober.csv'
file1 = open(csvfile, 'r')
reader = csv.reader(file1)

#apt-get install python-mysqldb -> command to install pymysql
mydb = MySQLdb.connect(host='172.16.1.73', user='prasanjit', passwd='prasan123', db='carwaledata')
cursor = mydb.cursor() 
count = 0
for row in reader:

	sql1 = "INSERT INTO PQMonthlyData VALUES (%d,%s,%d,%d)"


	try:
		#cursor.execute
		#("insert into [GoogleAnalytics].[dbo].[GAExperienceTrackersWeekly_New]
		#(Platform,TrackerHeader,TrackerType,TrackerCount,TrackerDate) 
		#values('M-Site','Location Sessions', 'Total Sessions',"+
		#data2['ga:sessions']+",'" + TrackerDate + "')")
	 	cursor.execute("INSERT INTO PQMonthlyData VALUES ("+row[0]+","+"'2016-10-01'"+","+row[2]+","+row[3]+")");
		mydb.commit()
		count = count + 1
		print "done",count
		#print row[0]
	except Exception as ex:
		mydb.rollback()
		print ex

print "Insertion complete"
	
mydb.close();

	



