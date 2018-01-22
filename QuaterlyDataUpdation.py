import pyodbc
import csv
import subprocess
import MySQLdb
csvfile = '/home/user/Downloads/MacroData.csv'
file1 = open(csvfile, 'r')
reader = csv.reader(file1)

mydb = MySQLdb.connect(host=destination_host_name, user=username, passwd=password, db=dbname)
cursor = mydb.cursor() 

for row in reader:
	YearQuarter = row[0].split(" ")
	Year = YearQuarter[0]
	Quarter = YearQuarter[1].replace("Q", "")

	sql1 = "INSERT INTO indexQuarterlyData VALUES (%s,%s,%s,%s)"
	args1 = (Year,Quarter,"1",row[1])
	sql2 = "INSERT INTO indexQuarterlyData VALUES (%s,%s,%s,%s)"
	args2 = (Year,Quarter,"3",row[2])
	sql3 = "INSERT INTO indexQuarterlyData VALUES (%s,%s,%s,%s)"
	args3 = (Year,Quarter,"4",row[3])
	try:
	 	cursor.execute(sql1,args1)
		cursor.execute(sql2,args2)
		cursor.execute(sql3,args3)
		mydb.commit()
	except:
		mydb.rollback()
		print 'Exception occurs'

print "done"
	
mydb.close();

	



