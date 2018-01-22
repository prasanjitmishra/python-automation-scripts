import csv
import subprocess
import MySQLdb
import datetime

hostnameCW='172.16.0.17'
usernameCW='cwreadonly'
passwordCW='cwreadonly'
databaseCW='cwmasterdb'

hostnameCWP='172.16.11.19'
usernameCWP='prasanjit'
passwordCWP='prasan123'
databaseCWP='carwaledata'

def MySqlConnection(hostname, username, password, database):
	mydb = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database)
	try:
		mydb = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database)
	except Exception as ex:
	    print str(ex)
	else:
	    print "\n"+"Connected to Carwale Database"
	    return mydb;

def fetchDataFromCarwale(conn):
	cursor = conn.cursor()
	sql = '''
			select A.ID, A.IsDeleted,A.Used,A.New ,A.VCreatedOn ,A.VUpdatedOn , A.Discontinuation_date , 
			A.LaunchDate,B.CategoryItemValue as Price, B.LastUpdated as PlastUpdated
			from cwmasterdb.carversions A 
			left join (
			            SELECT * FROM cwnewcar.newcarshowroomprices 
			where CityId = 10 AND CategoryItem = 2
			  ) B ON A.ID = B.CarVersionId
			order by A.ID'''

	try:
		cursor.execute(sql)
	except Exception as ex:
		print ex

	records = cursor.fetchall()
	conn.close()
	return records

def insertIntoCWPTable(conn,data):
	sql = "truncate Budgetwise_carversionData"
	cursor = conn.cursor()
	try:
		cursor.execute(sql)
		print "truncated Budgetwise_carversionData"
	except Exception as ex:
		print ex

	print "starting insertion to Budgetwise_carversionData"	
	sql = "INSERT INTO Budgetwise_carversionData VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

	for row in data:
		try:
			args = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9])
			# print args
			cursor.execute(sql,args);
			conn.commit()
		except Exception as ex:
			conn.rollback()
			print ex
	conn.close()

def main():
	conn = MySqlConnection(hostnameCW, usernameCW, passwordCW, databaseCW)
	# print conn
	data = fetchDataFromCarwale(conn)
	# print data
	conn = MySqlConnection(hostnameCWP, usernameCWP, passwordCWP, databaseCWP)
	insertIntoCWPTable(conn,data)

if __name__ =='__main__':
    main()
