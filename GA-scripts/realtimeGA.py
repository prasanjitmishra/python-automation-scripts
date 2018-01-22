#!/usr/bin/python
import httplib2
import datetime
import time
import MySQLdb
from analytics_service_object import initialize_service

# 1. Create and Execute a Real Time Report
# An application can request real-time data by calling the get method on the Analytics service object.
# The method requires an ids parameter which specifies from which view (profile) to retrieve data.
# For example, the following code requests real-time data for view (profile) ID 56789.


# 2. Print out the Real-Time Data
# The components of the report can be printed out as follows:

def print_realtime_report(results):
  print '**Real-Time Report Response**' 
  print_report_info(results)
  print_query_info(results.get('query'))
  print_profile_info(results.get('profileInfo'))
  print_column_headers(results.get('columnHeaders'))
  print_data_table(results)
  print_totals_for_all_results(results)

def print_data_table(results):
  print 'Data Table:'
  # Print headers.
  output = []
  for header in results.get('columnHeaders'):
    output.append('%30s' % header.get('name'))
  print ''.join(output)
  # Print rows.
  if results.get('rows', []):
    for row in results.get('rows'):
      output = []
      for cell in row:
        output.append('%30s' % cell)
      print ''.join(output)
  else:
    print 'No Results Found'

def print_column_headers(headers):
  print 'Column Headers:'
  for header in headers:
    print 'Column name           = %s' % header.get('name')
    print 'Column Type           = %s' % header.get('columnType')
    print 'Column Data Type      = %s' % header.get('dataType')

def print_query_info(query):
  if query:
    print 'Query Info:'
    print 'Ids                   = %s' % query.get('ids')
    print 'Metrics:              = %s' % query.get('metrics')
    print 'Dimensions            = %s' % query.get('dimensions')
    print 'Sort                  = %s' % query.get('sort')
    print 'Filters               = %s' % query.get('filters')
    print 'Max results           = %s' % query.get('max-results')

def print_profile_info(profile_info):
  if profile_info:
    print 'Profile Info:'
    print 'Account ID            = %s' % profile_info.get('accountId')
    print 'Web Property ID       = %s' % profile_info.get('webPropertyId')
    print 'Profile ID            = %s' % profile_info.get('profileId')
    print 'Profile Name          = %s' % profile_info.get('profileName')
    print 'Table Id              = %s' % profile_info.get('tableId')

def print_report_info(results):
  print 'Kind                    = %s' % results.get('kind')
  print 'ID                      = %s' % results.get('id')
  print 'Self Link               = %s' % results.get('selfLink')
  print 'Total Results           = %s' % results.get('totalResults')

def print_totals_for_all_results(results):
  totals = results.get('totalsForAllResults')
  for metric_name, metric_total in totals.iteritems():
    print 'Metric Name  = %s' % metric_name
    print 'Metric Total = %s' % metric_total

def main():
  service = initialize_service()

  now = datetime.datetime.now()
  today = datetime.date.today()
  curr_hour = now.hour
  curr_min = now.minute

  try:
    results = service.data().realtime().get(
      ids='ga:112880436',
      metrics='rt:activeUsers',
      dimensions='rt:medium').execute()
    others = results['rows'][0][1]
    organic = results['rows'][1][1]
  except TypeError, error:
    # Handle errors in constructing a query.
    print ('There was an error in constructing your query : %s' % error)

  # # except httplib2.HttpError, error:
  # #   # Handle API errors.
  # #   print ('Arg, there was an API error : %s : %s' %
  # #          (error.resp.status, error._get_reason()))

  mydb = MySQLdb.connect(host='172.16.11.19', user='prasanjit', passwd='prasan123', db='carwaleprotest')
  cursor = mydb.cursor()

  sql = """insert into GA_RealTimeActiveUsers values(%s,%s,%s,%s,%s)"""
  args = (today,curr_hour,curr_min,organic,others)
  try:
      cursor.execute(sql,args)
      mydb.commit()
  except Exception as ex:
      print str(ex)

if __name__ =='__main__':
    main()