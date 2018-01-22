#!/usr/bin/python
import datetime
import time
import MySQLdb
from datetime import timedelta
date = today = datetime.date.today()
year = date.year

hostname='172.16.11.19'
username='prasanjit'
password='prasan123'
database='carwaleprotest'

def connectDB(host,user,passwd,db):
	try:
	    mydb = MySQLdb.connect(host, user, passwd, db)
	    print "CONNECTED TO CARWALEPRO DB"
	    return mydb
	except Exception as ex:
	    print "ERROR IN CONNECTION : "+str(ex)

def deleteThisYearData(mydb):
	sql = """delete from BudgetwiseVersionDetails_duplicate where Year = %s;"""
	args = (year)
	cursor = mydb.cursor()

	try:
		cursor.execute(sql,args)
		mydb.commit()
		print "deleted data for year : "+str(year)
	except Exception as ex:
	    print "Error while deleting records from table : "+str(ex)

def insertDataIntoTable(mydb):
	sql = """INSERT INTO BudgetwiseVersionDetails_duplicate (SELECT cv.ID,cv.VPrice,%s,cvpm.ID
	FROM carwaleprotest.carversions cv
	join Budgetwise_carversion_price_master as cvpm on  cv.VPrice between cvpm.MinPrice and cvpm.MaxPrice
	where New = 1 AND IsDeleted <> 1)""";
	
	args = (year)
	cursor = mydb.cursor()

	try:
		cursor.execute(sql,args)
		mydb.commit()
		print "insered data for year : "+str(year)
	except Exception as ex:
	    print str(ex)


def main():
	print "Date : "+str(today)
	print "Updation for Opportunies Module :"
	print "Table : BudgetwiseVersionDetails"
	mydb = connectDB(hostname,username,password,database)
	deleteThisYearData(mydb)
	insertDataIntoTable(mydb)

	mydb.close()

if __name__ =='__main__':
    main()