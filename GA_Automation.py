from analytics_service_object import initialize_service
import datetime
import csv
import pyodbc
import sys
import logging
import time

CONN_STRING = "DRIVER={SQL Server};SERVER=172.16.0.14;UID=sa;PWD=p@ssw0rd"

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



def main():
    print "hello"
    service = initialize_service()
    cnxn = pyodbc.connect(CONN_STRING)
    cursor = cnxn.cursor()

    today = datetime.date.today()

