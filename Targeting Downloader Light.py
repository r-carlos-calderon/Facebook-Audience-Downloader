import requests
import datetime
import pandas as pd
import json
import os
import urllib.parse

# UNIVERSAL VARS
today = datetime.date.today()
audit_date = str(today)
folder = 'ADSETS_TARGETING'
path = f'{folder}/{audit_date}'
if not os.path.exists(f'{path}'):
    os.makedirs(f'{path}')
file_path = f'{path}'

# CONFIGS <FIELDS> documentation ref: https://thd.co/30cb02m - <LIMIT> MIN = 25, MAX = 5000,
# DEFAULTS TO MIN WHEN NOT DEFINED. THE QUANTITY AND COMPLEXITY OF FIELDS MAY REQUIRE A LOWER LIMIT
get_config = open('config.json')
set_config = json.load(get_config)
get_config.close()
token = set_config['token']
business_account = set_config['business_account']
adset_fields = set_config['adset_fields']
adset_limit = set_config['adset_limit']
FBGraphRequest = 'https://graph.facebook.com/v7.0'
params = {
    'fields': adset_fields,
    'limit': adset_limit,
    'access_token': token
}
request_params = urllib.parse.urlencode(params, doseq=True)

# DISCOVER ALL AD ACCOUNTS IN BUSINESS ACCOUNT (STEP 1)
res = requests.get(
    f'{FBGraphRequest}/{business_account}/owned_ad_accounts?fields=id,name&access_token={token}&limit=500')
r1 = res.text
r2 = json.loads(r1)
r3 = json.dumps(r2['data'])
r4 = json.loads(r3)
df = pd.DataFrame(r4)

# BEGIN DOWNLOADING ALL AUDIENCE METADATA
business_adsets = []
business_filename = f'{business_account}_adsets_{audit_date}'  # NAME OF FILE FOR ALL AUDIENCES IN BUSINESS ACCOUNT
for ind_a in df.index:  # ITERATE THROUGH ALL AD ACCOUNTS FROM STEP 1 TO DISCOVER ALL AUDIENCES (STEP 2)
    acct = str(df['id'][ind_a])
    res = requests.get(
        f'{FBGraphRequest}/{acct}/adsets?{request_params}')
    r1 = res.text
    r2 = json.loads(r1)
    r3 = r2['data']
    num_results = len(r3)
    # SKIP ACCOUNTS WITH NO CUSTOM AUDIENCES
    if num_results != 0:
        while True:  # GET NEXT PAGE RESULTS (GRAPH API ENDPOINT) REF: https://thd.co/2WiLrf2
            try:
                for pg_results in r2['data']:
                    business_adsets.append(pg_results)
                # ATTEMPT TO MAKE A REQUEST FOR THE NEXT PAGE OF DATA, IF IT EXISTS
                get_next = r2['paging']['next']
                r2 = requests.get(
                    f'{get_next}').json()
            except KeyError:
                # WHEN THERE ARE NO MORE PAGES (['paging']['next']), BREAK THE LOOP
                break

# SAVE AUDIENCE METADATA FROM ALL AD ACCOUNTS AS JSON FILE
business_adsets_pjson = json.dumps(business_adsets, indent=2)  # CONVERTS BOOLEAN VALUES TO TEXT STRINGS
print(business_adsets_pjson, file=open(f'{file_path}/{business_filename}.json', 'w'))
