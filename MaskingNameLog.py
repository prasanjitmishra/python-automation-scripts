#!/usr/bin/env python

import pyodbc
cnx = pyodbc.connect("DSN=CWDB;UID=sa;PWD=p@ssw0rd")
cursor = cnx.cursor()

# cursor.execute("select * from carwale_com.dbo.carmakes;")
# CW_carmakerecords = cursor.fetchall()

# cursor.execute("select ID, MaskingName from carwale_com.dbo.carmodels where MaskingName != '';")
# CW_currentmaskingnames = cursor.fetchall()

# #cursor.execute("select * from carwale_com.dbo.MaskingNameUpdateLog where MaskingName REGEXP '^[a-zA-Z0-9]' and MaskingName NOT REGEXP '[[:space:]]';")
# cursor.execute("select * from carwale_com.dbo.MaskingNameUpdateLog where MaskingName != '' and ModelId  != 545;")
# CW_historicmaskingnames = cursor.fetchall()

cursor.execute("select * from carwale_com.dbo.carmodels where MaskingName !='';")
CW_carmodelrecords = cursor.fetchall()

cursor.execute("select * from carwale_com.dbo.carversions;")
CW_carversionrecords = cursor.fetchall()

cursor.close()

import datetime

thisdate = datetime.datetime.today().strftime('%Y-%m-%d')

carmake_csv = 'CW_carmakes'+thisdate+'.csv'
currentmasking_csv = 'currentmaskingname'+thisdate+'.csv'
historicmasking_csv = 'historicmaskingname'+thisdate+'.csv'
carmodel_csv = 'cw_carmodels'+thisdate+'.csv'
carversion_csv = 'cw_carversions'+thisdate+'.csv'

import csv
import subprocess

carmakenames_csv = '/var/Data/'+carmake_csv
currentmaskingnames_csv = '/var/Data/'+currentmasking_csv
historicmaskingnames_csv = '/var/Data/'+historicmasking_csv
carmodelnames_csv = '/var/Data/'+carmodel_csv
carversionnames_csv = '/var/Data/'+carversion_csv


# with open(carmakenames_csv, "w") as output:
# 	writer = csv.writer(output, lineterminator='\n')
# 	writer.writerows(CW_carmakerecords)
	
# 	subprocess.call(['chmod', '0777', carmakenames_csv])
	


# with open(currentmaskingnames_csv, "w") as output:
# 	writer = csv.writer(output, lineterminator='\n')
# 	writer.writerows(CW_currentmaskingnames)
	
# 	subprocess.call(['chmod', '0777', currentmaskingnames_csv])
	

# with open(historicmaskingnames_csv, "w") as output:
# 	writer = csv.writer(output, lineterminator='\n')
# 	writer.writerows(CW_historicmaskingnames)
	
# 	subprocess.call(['chmod', '0777', historicmaskingnames_csv])


# with open(carmodelnames_csv, "w") as output:
# 	writer = csv.writer(output, lineterminator='\n')
# 	writer.writerows(CW_carmodelrecords)
	
# 	subprocess.call(['chmod', '0777', carmodelnames_csv])


# with open(carversionnames_csv, "w") as output:
# 	writer = csv.writer(output, lineterminator='\n')
# 	writer.writerows(CW_carversionrecords)
	
# 	subprocess.call(['chmod', '0777', carversionnames_csv])



import MySQLdb
mydb = MySQLdb.connect(host='172.16.1.73', user='prasanjit', passwd='prasan123', db='carwaleprotest')
cursor = mydb.cursor()
# cursor.execute('delete from gatestmodels')


# currentmaskingnames = csv.reader(file(currentmaskingnames_csv))
# for currentmaskingname in currentmaskingnames:
			
# 	record = cursor.execute("""INSERT INTO gatestmodels values (%s,%s)""", (currentmaskingname))
# 	mydb.commit()


# cursor.execute('select ID, MaskingName from gatestmodels')

# maskingnames = cursor.fetchall()

# historicmaskingnames = csv.reader(file(historicmaskingnames_csv))

# for historicmaskingname in historicmaskingnames:
# 		bool = True
# 		for maskingname in maskingnames:
# 			if historicmaskingname[1] == maskingname[1]:
# 				bool = False
# 				break
# 		if bool == True:
# 			record = cursor.execute("""INSERT INTO gatestmodels values (%s,%s)""", (historicmaskingname))
# 			mydb.commit()



# cursor.execute('select ID from carmakes ORDER BY ID DESC LIMIT 1')

# lastmakerecord = cursor.fetchall()


# for CW_carmakerecord in CW_carmakerecords:
# 	if CW_carmakerecord[0] > lastmakerecord[0][0]:
# 		newrecord = cursor.execute("""INSERT INTO carmakes values(%s,%s,%s,%s,%s,%s)""", (CW_carmakerecord[0], '', CW_carmakerecord[1], CW_carmakerecord[15], CW_carmakerecord[17], CW_carmakerecord[16

# 			]))
# 		mydb.commit()			

#Model Table Updation
# cursor.execute('select ID from carmodels ORDER BY ID DESC LIMIT 1')
# lastmodelrecord = cursor.fetchall()

# for CW_carmodelrecord in CW_carmodelrecords:
# 	if CW_carmodelrecord[0] > lastmodelrecord[0][0]:
# 		newrecord = cursor.execute("""INSERT INTO carmodels values(%s,%s,%s,%s,%s,%s)""",(CW_carmodelrecord[0],CW_carmodelrecord[1],'',CW_carmodelrecord[2],CW_carmodelrecord[24],CW_carmodelrecord[44]))
# 		mydb.commit()
# 	else:
# 		print "Model Table already Updated"

cursor.execute('select ID from carversions ORDER BY ID DESC LIMIT 1')
lastversionrecord = cursor.fetchall()

for CW_carversionrecord in CW_carversionrecords:
	if int(CW_carversionrecord[0]) > int(lastversionrecord[0][0]):
		CarMakeId = cursor.execute('select carmodels.CarMakeId from carversions join carmodels on carversions.CarModelId = carmodels.ID where carversions.CarModelId='+str(CW_carversionrecord[2]))
		
		newrecord = cursor.execute("""INSERT INTO carversions values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(CW_carversionrecord[0],CW_carversionrecord[1],CarMakeId,CW_carversionrecord[2],CW_carversionrecord[3],CW_carversionrecord[4],CW_carversionrecord[22],CW_carversionrecord[23],CW_carversionrecord[24],CW_carversionrecord[29],CW_carversionrecord[42]))
		mydb.commit()
	else:
	 	print "Carversion Table already Updated"

mydb.close()
print "Done"


