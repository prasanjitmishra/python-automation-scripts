#!/usr/bin/python
from analytics_service_object import initialize_service
import datetime
import time
from datetime import timedelta
import pyodbc
import sys
import os
import io
import logging
import csv
import subprocess
import MySQLdb

CONN_STRING = "DRIVER={FreeTDS};SERVER=host_server_name;UID=username;PWD=password;PORT=1433;"

def get_accounts_ids(service):
    accounts_ids = []
    try:
        accounts = service.management().accounts().list().execute()
        if accounts.get('items'):
            for account in accounts['items']:
                accounts_ids.append(account['id'])
    except:
        print("Error Here in accounts")

    return accounts_ids

def get_webproperties(service,account_id):      
    webproperties_items = []
    try:
        webproperties = service.management().webproperties().list(accountId=account_id).execute()   
        webproperties_items = webproperties['items']        
    except:
        print("Error Here in Webproperties")    
    return webproperties_items


def get_profiles(service,account_id,webproperty_id):
    profiles_items = []
    try:
        profiles = service.management().profiles().list(accountId=account_id,webPropertyId=webproperty_id).execute()
        profiles_items = profiles['items']
    except:
        print("Error Here")
    return profiles_items

def get_data(service, profile_id, start_date, end_date,segment):
    ids = "ga:" + profile_id
    metrics = "ga:users,ga:sessions"
    samplingLevel = 'HIGHER_PRECISION'
    data = service.data().ga().get(ids=ids, start_date=start_date, end_date=end_date, metrics=metrics, samplingLevel=samplingLevel,segment=segment).execute()
    return data["totalsForAllResults"]

def get_data2(service, profile_id, start_date, end_date,filters):
    ids = "ga:" + profile_id
    metrics = "ga:screenviews,ga:pageViews"
    samplingLevel = 'HIGHER_PRECISION'
    data2 = service.data().ga().get(ids=ids, start_date=start_date, end_date=end_date, metrics=metrics, samplingLevel=samplingLevel,filters=filters).execute()
    return data2["totalsForAllResults"]

def get_maskingNames_carwale(cursor,file):
    cursor.execute("""select cmo.Id as CarModelId,
                    replace(replace(lower(cm.Name),' ',''),'-','') as MakeMaskingName,
                    cmo.MaskingName as ModelMaskingName 
                    from Carwale_com.dbo.CarMakes cm 
                    join Carwale_com.dbo.CarModels cmo on cmo.CarMakeId = cm.id
                    where (cmo.MaskingName LIKE '%201%' OR cmo.MaskingName not Like '%199[1-9]%')
                    and cmo.IsDeleted !=1""")
    try:
        result = cursor.fetchall()
        file.write("\n"+"Fetched masking names from Carwale Database")
        return result;
    except Exception as ex:
        file.write("\n"+"Error : " + str(ex));
        return null
    

def insert_into_maskingmname_table(masking_names,file):
    try:
        mydb = MySQLdb.connect(host=destination_host_name, user=username, passwd=password, db=dbname)
        cursor = mydb.cursor()
        file.write("\n"+"CONNECTED TO CARWALEPRO DB")
    except Exception as ex:
        file.write("\n"+"ERROR IN CONNECTION : "+str(ex))

    if masking_names is not None:
        cursor.execute("""truncate GA_ModelMaskingName""")
        file.write("\n"+"Truncated Masking Name Table")
        count = 0
        file.write("\n"+"Inserting New Masking Names : Started, Count : "+str(count))
        for masking_name in masking_names: 
            sql = """insert into GA_ModelMaskingName values(%s,%s,%s)"""
            args = (masking_name[0],masking_name[2],masking_name[1])
            try:
                cursor.execute(sql,args)
                mydb.commit()
                count = count + 1
            except Exception as ex:
                print str(ex)
        file.write("\n"+"Inserting New Masking Names : Ended, Count : "+str(count))
    mydb.close()

def connect_MySql_DB(hostname,username,password,database):
    try:
        mydb = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database)
        cursor = mydb.cursor()             
    except Exception as ex:
        # print "ERROR IN CONNECTION"
        print "ERROR IN CONNECTION : "+str(ex)
        return False
    else:
        # print "CONNECTED to carwalepro database"    
        return cursor

def get_GA_data_daily(from_date,to_date,file):
    mydb = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database)
    cursor = mydb.cursor()

    cursor.execute(""" SELECT ModelId,ModelMaskingName,MakeMaskingName
                 FROM GA_ModelMaskingName
                 WHERE ModelId NOT IN
                (SELECT ModelId FROM GA_WebsiteMonthly WHERE date = %s)""",(from_date))
    masking_names = cursor.fetchall();
    
    count = 0
    service = initialize_service()
    file.write("\n"+"Initialized GA Service")
    if masking_names is not None:
        file.write("\n"+"Getting & Inserting GA Data : Started, Count : "+str(count))
        for masking_name in masking_names: 
            try:
                segment = "sessions::condition::ga:pagePath=~/"+str(masking_name[2])+"-cars/"+str(masking_name[1])+"/"
                filters = "ga:pagePath=~/"+str(masking_name[2])+"-cars/"+str(masking_name[1])+"/"
                data = get_data(service, '529535', from_date, to_date, segment)
                data3 = get_data2(service, '529535', from_date, to_date, filters)

                sql = """insert into GA_WebsiteMonthly values(%s,%s,%s,%s,%s)"""
                args = [masking_name[0], from_date, data3['ga:pageViews'], data['ga:users'], data['ga:sessions']]
                
                cursor.execute(sql,args)
                mydb.commit()
                count = count + 1
            except Exception as ex:
                file.write("\n"+"ERROR : "+str(ex))

    file.write("\n"+"Getting GA Data : Ended, Count : "+str(count))
    mydb.close()     

def convert_daily_data_to_csv(csvFile,file,from_date,to_date):
    mydb = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database)
    cursor = mydb.cursor()
    
    args = (from_date,to_date,file)
    cursor.execute("""SELECT ModelId,Date,Pageviews,Users,Sessions FROM carwaledata.GA_WebsiteMonthly
                    where date between %s and %s""",args)
    records = cursor.fetchall()
    if records is not None:
        file.write("\n"+"Writing data into CSV file for : START :"+str(from_date)+"_"+str(to_date))
        with open(csvFile, "w") as output:
            writer = csv.writer(output, lineterminator='\n')
            writer.writerows(records)
            subprocess.call(['chmod', '0777', csvFile])
    file.write("\n"+"Writing : END : "+str(from_date)+"_"+str(to_date))
    
def main():
    today = datetime.date.today()
    # required_date = (today + timedelta(days=-2)).strftime('%Y-%m-%d')
    from_date = "2015-01-01"
    to_date = "2015-01-31"

    logFile = '../GAUpdationLogs/GA_data_'+from_date+"_"+to_date+'.log'
    csvFile = '../GA_CSV_data/GA_data_'+from_date+"_"+to_date+'.csv'

    with io.FileIO(logFile, "w") as file:
        file.write("Date : "+from_date+"_"+to_date)

    file=open(logFile, "a+")

    cnxn = pyodbc.connect(CONN_STRING)
    try:
        cursor = cnxn.cursor()
    except Exception as ex:
        print str(ex)
    else:
        file.write("\n"+"Connected to Carwale Database")

    carwale_masking_names = get_maskingNames_carwale(cursor,file)    
    insert_into_maskingmname_table(carwale_masking_names,file)
    get_GA_data_daily(from_date,to_date,file)
    
    convert_daily_data_to_csv(csvFile,file,from_date,to_date)
    
if __name__ =='__main__':
    main()