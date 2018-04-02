#!/usr/local/bin/python
import pandas as pd
import vertica_python
import httplib2
import datetime
import ml_metrics, string, re, pylab as pl
from apiclient import discovery
import os

from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = ('https://www.googleapis.com/auth/drive'
'https://www.googleapis.com/auth/spreadsheets')
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

def get_my_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    print('Checking credentials')
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentialsmy = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentialsmy = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


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



    credentials = get_my_credentials()
    http = credentials.authorize(httplib2.Http())
    credentials.refresh(http)
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                        'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                                  discoveryServiceUrl=discoveryUrl)
    spreadsheetId = '1Gz8iJRDcgmedoAyJtnXd3wKBiazI_xc-Z2gAKjC-FJ4'
    # gc = gspread.authorize(credentials)
    # sht1 = gc.open_by_key(spreadsheetId)
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheetId).execute()
    sheets = sheet_metadata.get('sheets', '')
    title = dict([i.get("properties", {}).get("title", "Sheet1"),i.get("properties", {}).get("sheetId", 0)] for i in sheets)
    # sheet_id = sheets[0].get("properties", {}).get("sheetId", 0)
    now = str(datetime.datetime.now().strftime("%Y-%m-%d"))

    if not os.path.exists(r'csv/'+str(now)):
        os.makedirs(r'csv/'+str(now))

    print(title)
    # result=sht1.worksheet(sheets[0])
    # print(result.range(1,1,4,2))
    with vertica_python.connect(**conn_info_ndb45_socialquantum_com_5433) as connection:
        key=['193','180','181']
        for x in key:
            # 2017-02-18
            print(x)
            cur = connection.cursor()

            cur.execute("""   select app,platform,advertising_id,lc,sum(pay_in_dollars) as pay_in_dollars from (select name as app,platform,advertising_id,case when v >=20 then 'more_then_20$' else 'else' end as lc, v as pay_in_dollars from (select key,s,floor(sum(v)/100) as v from mtu where key in ('""" +str(x)+"""') and v is not null group by s,key)a inner join (select key,s,count(s) over (partition by s,app) as f,count(s) over (partition by app,platform,lc) as need ,advertising_id,platform,lc  from tas_ucc where key in ('""" +str(x)+"""') and advertising_id not in ('null','limited') group by s,platform,advertising_id,app,lc,key)b on a.s=b.s and a.key=b.key and f<=9 inner join (select id,name from keys group by id,name)k on b.key=k.id group by name,platform,advertising_id,v)a group by app,platform,advertising_id,lc   """)
            num_fields = len(cur.description)
            field_names = [i[0] for i in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=field_names)
            cur.close()
            df['lc']=df['app']+'_'+df['platform']+'_'+df['lc']
            location=df.lc.unique()
            for x in location:
                print(x)
                local = df.loc[df['lc'] == x, ['advertising_id', 'pay_in_dollars']].reset_index(drop=True)
                rangeend=len(local.index)
                name = re.sub('[^a-zA-Z0-9_]', '', str(x))
                print('its okey')
                print(name)
                local.to_csv("csv/"+str(now)+"/"+str(name)+".csv", header= True ,index=False, encoding='utf-8')
                print('done')
                try :
                    sheet_id=title[str(name)]
                    print(sheet_id)
                    # sheet=(i.find(str(name)) for i in title)

                    # sheet_id = i.get("properties", {})
                    #
                    #     sheets[0].get("properties", {}).get("sheetId", 0)
                    print('im clearing')
                    # sht1.worksheet(name).clear()
                    result = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body={"requests": [  { "updateCells": { "range":
                    {"sheetId": sheet_id }, "fields": "userEnteredValue"  } } ]  }).execute(
                except (ValueError,KeyError):
                        result = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId,
                          body={"requests": [ { "addSheet": {
                                                "properties": { "title": str(name) } } } ]
                          } ).execute()
                        print('factchecking')
                service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheetId, body={"valueInputOption": "USER_ENTERED", "data": [{ "range": "" + str(name) + "!R1C1:R1C2", "majorDimension": "ROWS",  "values": [list(local.columns.values)] } ] } ).execute()
                # print('start writhing')
                all=[]
                for index, row in local.iterrows():
                        f=str(row['advertising_id']),str(row['pay_in_dollars'])
                        all.append(list(f))
                print(len(all))
                print(all)
                for i in all:
                    print(i)

                result = service.spreadsheets().values().batchUpdate(
                                    spreadsheetId=spreadsheetId, body={
                                        "valueInputOption": "USER_ENTERED",
                                        "data": [

                                            {
                                                "range": "" + str(name) + "!R2C1:R" + str(len(all) + 2) + "C2",
                                                "majorDimension": "ROWS",
                                                "values": [i for i in all]
                                            }
                                        ]
                                    }
                                ).execute()
            print('end '+str(x))

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