#!/usr/local/bin/python
import pandas as pd
import vertica_python
import httplib2
import gspread
import datetime
import ml_metrics, string, re, pylab as pl
from apiclient import discovery
import os
import my_testapi as api

from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage




def main():

    conn_info_db105_socialquantum_com_5433 = {
            'host': 'db105.socialquantum.com',
            'port': 5433,
            'user': 'cockpit_admin',
            'password': '35jEKDbLvh5bcT',
            'database': 'stats3'
        }

    conn_info_ndb45_socialquantum_com_5433 = {
            'host': 'ndb45.socialquantum.com',
            'port': 5433,
            'user': 'cockpit_admin',
            'password': '35jEKDbLvh5bcT',
            'database': 'stats_int'
        }

    credentials = api.get_my_credentials()
    http = credentials.authorize(httplib2.Http())
    credentials.refresh(http)
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                        'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                                  discoveryServiceUrl=discoveryUrl)
    spreadsheetId = '1_gOKbuA_be_TI_aeW4KlxX0PGuY1uVwuIaR2ispp7fQ'
    gc = gspread.authorize(credentials)
    sht1 = gc.open_by_key(spreadsheetId)
    worksheets=sht1.get_worksheet(0)
    values_list = worksheets.col_values(1)
    print(values_list)

    # sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
    # print(sheet_metadata)
    # sheets = sheet_metadata.get('sheets', '')
    # print(sheets)
#     with vertica_python.connect(**conn_info_ndb45_socialquantum_com_5433) as connection:
#         key=['193','180','181']
#         for x in key:
#             # 2017-02-18
#             print(x)
#             cur = connection.cursor()
#
#             cur.execute("""   select app,platform,advertising_id,lc,sum(pay_in_dollars) as pay_in_dollars from (select name as app,platform,advertising_id,case when v >=20 then 'more_then_20$' else 'else' end as lc, v as pay_in_dollars from (select key,s,floor(sum(v)/100) as v from mtu where key in ('""" +str(x)+"""') and v is not null group by s,key)a inner join (select key,s,count(s) over (partition by s,app) as f,count(s) over (partition by app,platform,lc) as need ,advertising_id,platform,lc  from tas_ucc where key in ('""" +str(x)+"""') and advertising_id not in ('null','limited') group by s,platform,advertising_id,app,lc,key)b on a.s=b.s and a.key=b.key and f<=9 inner join (select id,name from keys group by id,name)k on b.key=k.id group by name,platform,advertising_id,v)a group by app,platform,advertising_id,lc   """)
#             num_fields = len(cur.description)
#             field_names = [i[0] for i in cur.description]
#             rows = cur.fetchall()
#             df = pd.DataFrame(rows, columns=field_names)
#             cur.close()
#             df['lc']=df['app']+'_'+df['platform']+'_'+df['lc']
#             location=df.lc.unique()
#             for x in location:
#                 print(x)
#                 local = df.loc[df['lc'] == x, ['advertising_id', 'pay_in_dollars']].reset_index(drop=True)
#                 rangeend=len(local.index)
#                 name = re.sub('[^a-zA-Z0-9_]', '', str(x))
#                 print('its okey')
#                 print(name)
#                 local.to_csv("csv/"+str(now)+"/"+str(name)+".csv", header= True ,index=False, encoding='utf-8')
#                 print('done')
#                 try :
#                     sheet_id=title[str(name)]
#                     print(sheet_id)
#                     # sheet=(i.find(str(name)) for i in title)
#
#                     # sheet_id = i.get("properties", {})
#                     #
#                     #     sheets[0].get("properties", {}).get("sheetId", 0)
#                     print('im clearing')
#                     # sht1.worksheet(name).clear()
#                     result = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body={"requests": [  { "updateCells": { "range":
#                     {"sheetId": sheet_id }, "fields": "userEnteredValue"  } } ]  }).execute(
#                 except (ValueError,KeyError):
#                         result = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId,
#                           body={"requests": [ { "addSheet": {
#                                                 "properties": { "title": str(name) } } } ]
#                           } ).execute()
#                         print('factchecking')
#                 service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheetId, body={"valueInputOption": "USER_ENTERED", "data": [{ "range": "" + str(name) + "!R1C1:R1C2", "majorDimension": "ROWS",  "values": [list(local.columns.values)] } ] } ).execute()
#                 # print('start writhing')
#                 all=[]
#                 for index, row in local.iterrows():
#                         f=str(row['advertising_id']),str(row['pay_in_dollars'])
#                         all.append(list(f))
#                 print(len(all))
#                 print(all)
#                 for i in all:
#                     print(i)
#
#                 result = service.spreadsheets().values().batchUpdate(
#                                     spreadsheetId=spreadsheetId, body={
#                                         "valueInputOption": "USER_ENTERED",
#                                         "data": [
#
#                                             {
#                                                 "range": "" + str(name) + "!R2C1:R" + str(len(all) + 2) + "C2",
#                                                 "majorDimension": "ROWS",
#                                                 "values": [i for i in all]
#                                             }
#                                         ]
#                                     }
#                                 ).execute()
#             print('end '+str(x))
#
if __name__ == '__main__':
     main()





# result = service.spreadsheets().batchUpdate(
#     spreadsheetId=spreadsheetId, body={
#         "requests": [
#     {
#       "addSheet": {
#         "properties": {
#           "title": str(location),
#
#         }
#       }
#     }
#   ] }).execute()