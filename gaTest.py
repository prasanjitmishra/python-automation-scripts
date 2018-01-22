import datetime
import csv
import pyodbc
import sys
import logging
import time

#CONN_STRING = "DRIVER={SQL Server};SERVER=172.16.0.14;DATABASE=Carwale_com;UID=sa;PWD=p@ssw0rd"
CONN_STRING = "DRIVER={SQL Server};SERVER=172.16.0.14;UID=sa;PWD=p@ssw0rd"
#;DATABASE=GoogleAnalytics

# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
  """Get a service that communicates to a Google API.

  Args:
    api_name: The name of the api to connect to.
    api_version: The api version to connect to.
    scope: A list auth scopes to authorize for the application.
    key_file_location: The path to a valid service account p12 key file.
    service_account_email: The service account email address.

  Returns:
    A service that is connected to the specified API.
  """

  credentials = ServiceAccountCredentials.from_p12_keyfile(
    service_account_email, key_file_location, scopes=scope)

  http = credentials.authorize(httplib2.Http())

  # Build the service object.
  service = build(api_name, api_version, http=http)

  return service

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
    #service = initialize_service()
    cnxn = pyodbc.connect(CONN_STRING)
    cursor = cnxn.cursor()

    today = datetime.date.today()
    
    cursor.execute("select Id,Name,replace(replace(lower(Name),' ',''),'-','') as MakeMaskingName from Carwale_com.dbo.CarMakes WHERE New=1 and Name in ('bmw','chevrolet','fiat','ford','honda','hyundai','mahindra','maruti suzuki','mercedes-benz','mitsubishi','skoda','tata','toyota','audi','porsche','volkswagen','nissan','land rover','volvo','jaguar','renault','mini','ssangyong','datsun','isuzu')")#
    makes = cursor.fetchall()

    print makes

if __name__ =='__main__':
    main()
