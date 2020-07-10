import requests
import datetime
import pandas as pd
import json
import sys
import os
from pandas import json_normalize

# THIS SCRIPT DOWNLOADS AUDIENCES METADATA AS CSV AND JSON

# CONFIGS
get_config = open('config.json')
set_config = json.load(get_config)
get_config.close()
business_account = set_config['business_account']
token = set_config['token']

# UNIVERSAL PARAMS
today = datetime.date.today()
audit_date = today
add_date = {'audit_date': str(audit_date)}
path_output = 'AUDIENCE_TARGETING'
if not os.path.exists(f'{path_output}/{audit_date}'):
     os.makedirs(f'{path_output}/{audit_date}')
file_path = f'{path_output}/{audit_date}'

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
res = requests.get(f'https://graph.facebook.com/v7.0/{business_account}/owned_ad_accounts?access_token={token}&limit=500')
r1 = res.text
r2 = json.loads(r1)
r3 = json.dumps(r2['data'])
r4 = json.loads(r3)
df_acct = pd.DataFrame(r4)

# BEGIN DOWNLOADING ALL AUDIENCE METADATA
all_audiences_json = []
total_num_audiences = 0
for ind_a in df_acct.index:  # ITERATE THROUGH ALL AD ACCOUNTS FROM STEP 1 TO DISCOVER ALL AUDIENCES (STEP 3)
    acct = str(df_acct['account_id'][ind_a])
    res = requests.get(
         f'https://graph.facebook.com/v7.0/act_{acct}/customaudiences?fields=id&limit=5000&pretty=1&access_token={token}')
    r1 = res.text
    r2 = json.loads(r1)
    while True:  # GET NEXT PAGE RESULTS (GRAPH API ENDPOINT)
        try:
            for page_results in r2['data']:
                all_audiences_json.append(page_results)
                # Attempt to make a request to the next page of data, if it exists.
            r2 = requests.get(r2['paging']['next']).json()
        except KeyError:
            # When there are no more pages (['paging']['next']), break from the loop
            break
    r3 = json.dumps(all_audiences_json)
    r4 = json.loads(r3)
    df_audiences = pd.DataFrame(r4)
    ind_b = 0
    for ind_b in df_audiences.index:  # ITERATE THROUGH ALL AUDIENCES FROM STEP 3 >> DOWNLOAD AUDIENCE METADATA
        audience = str(df_audiences['id'][ind_b])
        res = requests.get(f'https://graph.facebook.com/v7.0/{audience}?fields=id%2Cname%2Cdescription%2Capproximate_count%2Ccustomer_file_source%2Cretention_days%2Csubtype%2Ctime_created%2Ctime_updated%2Cdata_source%2Cpermission_for_actions%2Cdelivery_status%2Coperation_status%2Clookalike_spec%2Clookalike_audience_ids%2Caccount_id%2Cads.limit(5000)%7Badset_id%2Cstatus%7D&access_token={token}')
        r1 = res.text
        r2 = json.loads(r1)
        r2.update(add_date)
        r3 = json.dumps(convert(r2))
        r4 = json.loads(r3)
        all_audiences_json.append(r4)
    num_audiences = ind_b + 1
    total_num_audiences += num_audiences
    sys.stdout.write(f'Downloaded {num_audiences} audiences metadata from act_{acct}\n')  # OUTPUT FOR LOG FILE
numacct = ind_a + 1
all_audience_pretty_json = json.dumps(all_audiences_json, indent=2)
print(all_audience_pretty_json, file=open(f'{file_path}/audiences_{audit_date}.json', 'w'))  # SAVE RAW AUDIENCE METADATA AS JSON
open_audiences = open(f'{file_path}/audiences_{audit_date}.json')
load_audiences = json.load(open_audiences)
open_audiences.close()
final_audiences = json_normalize(load_audiences)
final_audiences.to_csv(f'{file_path}/audiences_trimmed_{audit_date}.csv', columns=['id', 'name', 'description', 'approximate_count', 'customer_file_source', 'retention_days', 'subtype', 'time_created', 'time_updated', 'account_id', 'audit_date', 'data_source.type', 'data_source.sub_type', 'permission_for_actions.can_edit', 'permission_for_actions.can_see_insight', 'permission_for_actions.can_share', 'permission_for_actions.subtype_supports_lookalike', 'permission_for_actions.supports_recipient_lookalike', 'delivery_status.code', 'delivery_status.description', 'operation_status.code', 'operation_status.description', 'lookalike_spec.country', 'lookalike_spec.origin', 'ads.data', 'lookalike_audience_ids'], index=False, sep=',', encoding='utf-8')  # TRIM UNNECESSARY AUDIENCE METADATA AND SAVE AS CSV
sys.stdout.write(f'\nDownloaded total of {total_num_audiences} audiences across {numacct} accounts in business account #{business_account}.\n')  # OUTPUT FOR LOG FILE
# END DOWNLOADING ALL AUDIENCE METADATA
