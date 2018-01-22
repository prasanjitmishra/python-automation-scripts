import pyodbc
import csv
import subprocess
import MySQLdb
import datetime
csvfile = '/home/user/Desktop/comparisonData/tworoots.csv'
file1 = open(csvfile, 'r')
reader = csv.reader(file1)

#apt-get install python-mysqldb -> command to install pymysql
mydb = MySQLdb.connect(host='172.16.1.73', user='prasanjit', passwd='prasan123', db='carwaleprotest')
cursor = mydb.cursor() 
count = 0
for row in reader:
	try:
	 	#cursor.execute("INSERT INTO CarComparison_3Roots VALUES ("+'2016-10-01'+","+row[1]+","+row[2]+","+row[3]+","+row[4]+")");
		cursor.execute("INSERT INTO CarComparison_2Roots VALUES ('2016-10-01',"+row[1]+","+row[2]+","+row[3]+")");
		mydb.commit()
		#print "("+"2016-10-01"+","+row[1]+","+row[2]+","+row[3]+","+")"
		count = count + 1
		print "done-",count
		#one root is updated but 2 roots are remaining
	except Exception as ex:
		mydb.rollback()
		print ex

print "Insertion complete"
	
mydb.close();

	



