import requests
import datetime
import pandas as pd
import json
import sys
import os
from pandas import json_normalize
import urllib.parse

# UNIVERSAL VARS
today = datetime.date.today()
audit_date = str(today)
folder = 'AUDIENCE_TARGETING'
path = f'{folder}/{audit_date}'
if not os.path.exists(f'{path}'):
    os.makedirs(f'{path}')
file_path = f'{path}'
log = sys.stdout
sys.stdout = open(f'{file_path}/log_{audit_date}.txt', 'w')  # NAME OF LOG FILE
sys.stdout.write(f'TIMESTAMP, BUSINESS_ACCOUNT, AD_ACCOUNT_ID, AD_ACCOUNT_NAME, EVENT, NUM')  # OUTPUT FOR LOG FILE

# CONFIGS <FIELDS> documentation ref: https://thd.co/30cb02m - <LIMIT> MIN = 25, MAX = 5000,
# DEFAULTS TO MIN WHEN NOT DEFINED. THE QUANTITY AND COMPLEXITY OF FIELDS MAY REQUIRE A LOWER
get_config = open('config.json')
set_config = json.load(get_config)
get_config.close()
token = set_config['token']
business_account = set_config['business_account']
fields = set_config['fields']
limit = set_config['limit']
FBGraphRequest = 'https://graph.facebook.com/v7.0'
params = {
    'fields': fields,
    'limit': limit,
    'access_token': token
}
request_params = urllib.parse.urlencode(params, doseq=True)


# CHANGES BOOLEAN VALUES IN JSON RESPONSE to LOWERCASE STRING
def convert(obj):
    if isinstance(obj, bool):
        return str(obj).lower()
    if isinstance(obj, (list, tuple)):
        return [convert(item) for item in obj]
    if isinstance(obj, dict):
        return {convert(key): convert(value) for key, value in obj.items()}
    return obj


# DISCOVER ALL AD ACCOUNTS IN BUSINESS ACCOUNT (STEP 1)
res = requests.get(
    f'{FBGraphRequest}/{business_account}/owned_ad_accounts?fields=id,name&access_token={token}&limit=500')
r1 = res.text
r2 = json.loads(r1)
r3 = json.dumps(r2['data'])
r4 = json.loads(r3)
df = pd.DataFrame(r4)

# BEGIN DOWNLOADING ALL AUDIENCE METADATA
business_audiences = []
business_filename = f'{business_account}_audiences_{audit_date}'  # NAME OF FILE FOR ALL AUDIENCES IN BUSINESS ACCOUNT
for ind_a in df.index:  # ITERATE THROUGH ALL AD ACCOUNTS FROM STEP 1 TO DISCOVER ALL AUDIENCES (STEP 2)
    acct = str(df['id'][ind_a])
    acct_name = str(df['name'][ind_a])
    log_string = f'{business_account}, {acct}, {acct_name}'
    acct_filename = f'{acct}_audiences_{audit_date}'  # NAME OF FILE FOR AUDIENCES IN AD ACCOUNT
    res = requests.get(
        f'{FBGraphRequest}/{acct}/customaudiences?{request_params}')
    r1 = res.text
    r2 = json.loads(r1)
    num_results = len(r2['data'])
    # SKIP ACCOUNTS WITH NO CUSTOM AUDIENCES
    if num_results != 0:
        acct_audiences = []
        t1 = datetime.datetime.now()
        sys.stdout.write(f'\n{t1}, {log_string}, FETCHING, {num_results}')  # OUTPUT FOR LOG FILE
        log.write(f'\n{t1}, {log_string}, FETCHING, {num_results}')  # SCREEN OUTPUT FOR TROUBLESHOOTING
        while True:  # GET NEXT PAGE RESULTS (GRAPH API ENDPOINT) REF: https://thd.co/2WiLrf2
            try:
                for pg_results in r2['data']:
                    acct_audiences.append(pg_results)
                    business_audiences.append(pg_results)
                # ATTEMPT TO MAKE A REQUEST FOR THE NEXT PAGE OF DATA, IF IT EXISTS
                get_next = r2['paging']['next']
                r2 = requests.get(
                    f'{get_next}').json()
                num_results += len(r2['data'])
                t2 = datetime.datetime.now()
                sys.stdout.write(f'\n{t2}, {log_string}, FETCHING, {num_results}')  # OUTPUT FOR LOG FILE
                log.write(f'\n{t2}, {log_string}, FETCHING, {num_results}')  # SCREEN OUTPUT FOR TROUBLESHOOTING
            except KeyError:
                # WHEN THERE ARE NO MORE PAGES (['paging']['next']), BREAK THE LOOP
                break
        t3 = datetime.datetime.now()
        sys.stdout.write(f'\n{t3}, {log_string}, DOWNLOADED, {num_results}')  # OUTPUT FOR LOG FILE
        log.write(f'\n{t3}, {log_string}, DOWNLOADED, {num_results}')  # SCREEN OUTPUT FOR TROUBLESHOOTING
        # SAVE RAW AUDIENCE METADATA FROM SINGLE AD ACCOUNT AS JSON
        acct_audiences_pjson = json.dumps(acct_audiences, indent=2)
        print(acct_audiences_pjson, file=open(f'{file_path}/{acct_filename}.json', 'w'))

# SAVE AUDIENCE METADATA FROM ALL AD ACCOUNTS AS JSON FILE
total_count = len(business_audiences)
business_audiences_pjson = json.dumps(convert(business_audiences), indent=2)  # CONVERTS BOOLEAN VALUES TO TEXT STRINGS
print(business_audiences_pjson, file=open(f'{file_path}/{business_filename}.json', 'w'))

# SAVE AUDIENCE METADATA FROM ALL AD ACCOUNTS AS CSV FILE
open_audiences = open(f'{file_path}/{business_filename}.json')
load_audiences = json.load(open_audiences)
open_audiences.close()
final_audiences = json_normalize(load_audiences)
final_audiences['date'] = today
final_audiences.to_csv(f'{file_path}/{business_filename}.csv', index=False, sep=',', encoding='utf-8')

t4 = datetime.datetime.now()
sys.stdout.write(f'\n{t4}, {business_account}, ALL, ALL, DOWNLOADED, {total_count}')  # OUTPUT FOR LOG FILE
log.write(f'\n{t4}, {business_account}, ALL, ALL, DOWNLOADED, {total_count}')  # SCREEN OUTPUT FOR TROUBLESHOOTING
sys.stdout.close()

# SAVE OUTPUT LOG AS CSV FILE
open_log = pd.read_csv(f'{file_path}/log_{audit_date}.txt')
open_log['DATE'] = today
open_log.to_csv(f'{file_path}/log_{audit_date}.csv', index=None)
