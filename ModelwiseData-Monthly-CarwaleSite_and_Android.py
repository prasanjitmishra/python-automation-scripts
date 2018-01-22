from analytics_service_object import initialize_service
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
    service = initialize_service()
    cnxn = pyodbc.connect(CONN_STRING)
    cursor = cnxn.cursor()

    today = datetime.date.today()
    #today = datetime.date(day=1, month=9, year=2016)
    for i in range(0,1):
        

        print today
        
        if today.month==1:
            MonthEndint = datetime.date(day=31, month=12, year=today.year-1)
            MonthStartint = datetime.date(day=1, month=12, year=today.year-1)
            MonthStart = str(MonthStartint)
            MonthEnd = str(MonthEndint)
        else:   
            ThisMonthStart = datetime.date(day=1, month=today.month, year=today.year)
            MonthEndint = ThisMonthStart-datetime.timedelta(1)
            MonthStartint = datetime.date(day=1, month=MonthEndint.month, year=MonthEndint.year)
            MonthStart = str(MonthStartint)
            MonthEnd = str(MonthEndint)
        

        print MonthStart, MonthEnd


        # today = datetime.date.today()
        # TodayMonthStart = datetime.date(day=1, month=today.month-i, year=today.year)
        # MonthEndint = TodayMonthStart-datetime.timedelta(1)
        # MonthStartint = datetime.date(day=1, month=MonthEndint.month, year=MonthEndint.year)
        # MonthStart = str(MonthStartint)
        # MonthEnd = str(MonthEndint)
        # print MonthStart, MonthEnd

    	# makes = [1,2,3,4,5,6,7,8,9,10,12,13,14,15,16,17,19,20,22,34,39,40,41]
        # makes=['Bajaj','Aprilia','BMW','Ducati','Harley Davidson','Hero','Honda','Hyosung','KTM','Mahindra','Suzuki','Yamaha','Yo','TVS','Vespa','Kawasaki','LML','Moto Guzzi','Triumph','Indian','Hero Electric','Benelli','MV Agusta']
        cursor.execute("select Id,Name,replace(replace(lower(Name),' ',''),'-','') as MakeMaskingName from Carwale_com.dbo.CarMakes WHERE New=1 and Name in ('bmw','chevrolet','fiat','ford','honda','hyundai','mahindra','maruti suzuki','mercedes-benz','mitsubishi','skoda','tata','toyota','audi','porsche','volkswagen','nissan','land rover','volvo','jaguar','renault','mini','ssangyong','datsun','isuzu')")#
       	makes = cursor.fetchall()

        for make in makes:
            cursor.execute("select Id, Name, MaskingName from Carwale_com.dbo.CarModels WHERE New=1 and isDeleted=0 and CarMakeId="+str(make.Id))
            models = cursor.fetchall()
            for model in models:
                    #print make.Name, model.Name
                    Makename = make.Name.replace("'","''")
                    ModelName = model.Name.replace("'","''")
                    print Makename, ModelName


                    #Carwale Desktop

                    segment = "sessions::condition::ga:pagePath=~/"+str(make.MakeMaskingName)+"-cars/"+str(model.MaskingName)+"/"
                    filters = "ga:pagePath=~/"+str(make.MakeMaskingName)+"-cars/"+str(model.MaskingName)+"/"
                    data = get_data(service, '529535', MonthStart, MonthEnd, segment)
                    data3 = get_data2(service, '529535', MonthStart, MonthEnd, filters)
                    
                    cursor.execute("insert into [GoogleAnalytics].[dbo].[GAModelWiseMonthlyCarwale](Make,Model,ModelId,TrackerType,TrackerCount,Trackerdate) values('"+str(Makename)+"','"+str(ModelName)+"','"+str(model.Id)+"', 'PageViews',"+data3['ga:pageViews']+",'" + MonthEnd + "')")
                    
                    cursor.execute("insert into [GoogleAnalytics].[dbo].[GAModelWiseMonthlyCarwale](Make,Model,ModelId,TrackerType,TrackerCount,Trackerdate) values('"+str(Makename)+"','"+str(ModelName)+"','"+str(model.Id)+"', 'Users',"+data['ga:users']+",'" + MonthEnd + "')")
                    
                    cursor.execute("insert into [GoogleAnalytics].[dbo].[GAModelWiseMonthlyCarwale](Make,Model,ModelId,TrackerType,TrackerCount,Trackerdate) values('"+str(Makename)+"','"+str(ModelName)+"','"+str(model.Id)+"', 'Sessions',"+data['ga:sessions']+",'" + MonthEnd + "')")
                    
                    cnxn.commit()
                

                    #Carwale Android

                    segment2 = "sessions::condition::ga:screenName=~com.carwale.carwale.activities.newcars.ModelDetail."+str(make.Name)+"."+str(model.Name)+",ga:screenName=~com.carwale.carwale.activities.newcars.VersionDetails."+str(make.Name)+"."+str(model.Name)
                    filters2 = "ga:screenName=~com.carwale.carwale.activities.newcars.ModelDetail."+str(make.Name)+"."+str(model.Name)+",ga:screenName=~com.carwale.carwale.activities.newcars.VersionDetails."+str(make.Name)+"."+str(model.Name)
                    data2 = get_data(service, '79482817', MonthStart, MonthEnd, segment2)
                    data4 = get_data2(service, '79482817', MonthStart, MonthEnd, filters2)
                    
                    cursor.execute("insert into [GoogleAnalytics].[dbo].[GAModelWiseMonthlyCarwaleAndroid](Make,Model,ModelId,TrackerType,TrackerCount,Trackerdate) values('"+str(Makename)+"','"+str(ModelName)+"','"+str(model.Id)+"', 'ScreenViews',"+data4['ga:screenviews']+",'" + MonthEnd + "')")
                    
                    cursor.execute("insert into [GoogleAnalytics].[dbo].[GAModelWiseMonthlyCarwaleAndroid](Make,Model,ModelId,TrackerType,TrackerCount,Trackerdate) values('"+str(Makename)+"','"+str(ModelName)+"','"+str(model.Id)+"', 'Users',"+data2['ga:users']+",'" + MonthEnd + "')")
                    
                    cursor.execute("insert into [GoogleAnalytics].[dbo].[GAModelWiseMonthlyCarwaleAndroid](Make,Model,ModelId,TrackerType,TrackerCount,Trackerdate) values('"+str(Makename)+"','"+str(ModelName)+"','"+str(model.Id)+"', 'Sessions',"+data2['ga:sessions']+",'" + MonthEnd + "')")
                    
                    cnxn.commit()

                                
        if today.month==1:
            today = datetime.date(day=1, month=12, year=today.year-1)   
        else:
            today = datetime.date(day=1, month=today.month-1, year=today.year)      


if __name__ =='__main__':
    main()
