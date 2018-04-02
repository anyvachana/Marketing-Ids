from __future__ import print_function
import os
import vertica_python
import logging

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
print('Checking credentials')

def definition(key):
    if key in ['114','115','144','145']:
        print('dz3d ru connection')
        conn_info = {
            'host': 'db58.socialquantum.com',
            'port': 5433,
            'user': 'direct_dz3d',
            'password': 'MtsljIxKuvfaGPNmjbKMi',
            'database': 'stats3',
            # 10 minutes timeout on queries
            'read_timeout': 600,
            # default throw error on invalid UTF-8 results
            'unicode_error': 'strict',
            # SSL is disabled by default
            'ssl': False
        }
        connection = vertica_python.connect(**conn_info)
        return connection.cursor()
    if key in ['193']:
        print('dz3d int connection')
        conn_info = {
            'host': 'db58.socialquantum.com',
            'port': 5433,
            'user': 'direct_dz3dint',
            'password': 'WskjXNLadRWvQaceifZZT',
            'database': 'stats3',
            # 10 minutes timeout on queries
            'read_timeout': 600,
            # default throw error on invalid UTF-8 results
            'unicode_error': 'strict',
            # SSL is disabled by default
            'ssl': False
        }

        # simple connection, with manual close
        connection = vertica_python.connect(**conn_info)
        return connection.cursor()
    if key in ['174','173','203','170','171']:
        print('farm2 ru connection')
        conn_info = {
            'host': 'db58.socialquantum.com',
            'port': 5433,
            'user': 'direct_farm2',
            'password': 'JCZAthvQeHBvCSvBVcvOy',
            'database': 'stats3',
            'read_timeout': 600,
            'unicode_error': 'strict',
            'ssl': False
        }

        # simple connection, with manual close
        connection = vertica_python.connect(**conn_info)
        return connection.cursor()
    if key in ['191','188','189','187']:
        print('farm2 int connection')
        conn_info = {'host': 'ndb45.socialquantum.com',
                     'port': 5433,
                     'user': 'direct_farm2int',
                     'password': 'LAyrBdrjLRdowXbPXDypM',
                     'database': 'stats_int',
                     'read_timeout': 1200,
                     'unicode_error': 'strict',
                     'ssl': False
                     }

        # simple connection, with manual close
        connection = vertica_python.connect(**conn_info)
        return connection.cursor()

def definition_admin(key):
        if key in ['114', '115', '144', '145','174', '173', '203', '170', '171']:
            print(' ru connection')
            conn_info = {
                'host': 'db105.socialquantum.com',
                'port': 5433,
                'user': 'cockpit_admin',
                'password': '35jEKDbLvh5bcT',
                'database': 'stats3',
                # 10 minutes timeout on queries
                'read_timeout': 600,
                # default throw error on invalid UTF-8 results
                'unicode_error': 'strict',
                # SSL is disabled by default
                'ssl': False
            }
            connection = vertica_python.connect(**conn_info)
            return connection.cursor()
        if key in ['193','191', '188', '189', '187']:
            print(' int connection')
            conn_info = {
                'host': 'ndb45.socialquantum.com',
                'port': 5433,
                'user': 'cockpit_admin',
                'password': '35jEKDbLvh5bcT',
                'database': 'stats_int',
                # 10 minutes timeout on queries
                'read_timeout': 600,
                # default throw error on invalid UTF-8 results
                'unicode_error': 'strict',
                # SSL is disabled by default
                'ssl': False
            }

            # simple connection, with manual close
            connection = vertica_python.connect(**conn_info)
            return connection.cursor()



# dz3d ru
def connectionru():
    print('dz3d ru connection')
    conn_info = {
        'host': 'db105.socialquantum.com',
        'port': 5433,
        'user': 'cockpit_admin',
        'password': '35jEKDbLvh5bcT',
        'database': 'stats3',
                # 10 minutes timeout on queries
        'read_timeout': 600,
                # default throw error on invalid UTF-8 results
        'unicode_error': 'strict',
                # SSL is disabled by default
        'ssl': False
    }

    # simple connection, with manual close
    connection = vertica_python.connect(**conn_info)
    return connection.cursor()

# dz3d int
def connectionint():
    print('dz3d int connection')
    conn_info = {
        'host': 'db58.socialquantum.com',
        'port': 5433,
        'user': 'direct_dz3dint',
        'password': 'WskjXNLadRWvQaceifZZT',
        'database': 'stats3',
        # 10 minutes timeout on queries
        'read_timeout': 600,
        # default throw error on invalid UTF-8 results
        'unicode_error': 'strict',
        # SSL is disabled by default
        'ssl': False
    }

    # simple connection, with manual close
    connection = vertica_python.connect(**conn_info)
    return connection.cursor()
# farm2 ru
def connectionru_farm2():
    print('farm2 ru connection')
    conn_info = {
        'host':'db58.socialquantum.com',
        'port':5433,
        'user':'direct_farm2',
        'password':'JCZAthvQeHBvCSvBVcvOy',
        'database': 'stats3',
        'read_timeout': 600,
        'unicode_error': 'strict',
        'ssl': False
    }

    # simple connection, with manual close
    connection = vertica_python.connect(**conn_info)
    return connection.cursor()

# farm2 int
def connectionint_farm2():
    print('farm2 int connection')
    conn_info = {'host':'ndb45.socialquantum.com',
            'port':5433,
            'user':'direct_farm2int',
            'password':'LAyrBdrjLRdowXbPXDypM',
            'database': 'stats_int',
            'read_timeout': 1200,
            'unicode_error': 'strict',
            'ssl': False
    }

    # simple connection, with manual close
    connection = vertica_python.connect(**conn_info)
    return connection.cursor()


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
        else: # Needed only for compatibility with Python 2.6
            credentialsmy = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials
logging.getLogger('oauth2client.contrib.locked_file').setLevel(logging.ERROR)
print('google connection')

