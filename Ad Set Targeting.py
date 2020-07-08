import requests
import datetime
import pandas as pd
import json
import sys
import os
from pandas import json_normalize

# THIS SCRIPT DOWNLOADS TARGETING METADATA OF ADSETS AS CSV AND JSON

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

# BEGIN DOWNLOADING ALL ADSET TARGETING METADATA
all_adsets_json = []
total_num_adsets = 0
for ind_a in df_acct.index:  # ITERATE THROUGH ALL AD ACCOUNTS FROM STEP 1 TO DISCOVER ALL ADSETS (STEP 2)
    acct = str(df_acct['account_id'][ind_a])
    res = requests.get(f'https://graph.facebook.com/v7.0/act_{acct}/adsets?fields=id,account_id&limit=9999&access_token={token}')
    r1 = res.text
    r2 = json.loads(r1)
    r3 = json.dumps(r2['data'])
    r4 = json.loads(r3)
    df_adset = pd.DataFrame(r4)
    ind_b = 0
    for ind_b in df_adset.index:  # ITERATE THROUGH ALL ADSETS FROM STEP 2 >> DOWNLOAD ADSET TARGETING METADATA
        adset_id = str(df_adset['id'][ind_b])
        res = requests.get(f'https://graph.facebook.com/v7.0/{adset_id}?fields=id,status,targeting&access_token={token}')
        r1 = res.text
        r2 = json.loads(r1)
        r2.update(add_date)
        all_adsets_json.append(r2)
    num_adsets = ind_b + 1
    total_num_adsets += num_adsets
    sys.stdout.write(f'Downloaded {num_adsets} adsets metadata from act_{acct}\n')  # OUTPUT FOR LOG FILE
all_adsets_pretty_json = json.dumps(all_adsets_json, indent=2)
print(all_adsets_pretty_json, file=open(f'{file_path}/adset_targeting_{audit_date}.json', 'w'))  # SAVE RAW ADSET TARGETING METADATA AS JSON
open_adsets = open(f'{file_path}/adset_targeting_{audit_date}.json')
load_adsets = json.load(open_adsets)
open_adsets.close()
final_adsets = json_normalize(load_adsets)
final_adsets.to_csv(f'{file_path}/adset_targeting_trimmed_{audit_date}.csv', columns=['id', 'status', 'audit_date', 'targeting.age_max', 'targeting.age_min', 'targeting.custom_audiences', 'targeting.geo_locations.countries', 'targeting.geo_locations.location_types', 'targeting.publisher_platforms', 'targeting.facebook_positions', 'targeting.instagram_positions', 'targeting.device_platforms', 'targeting.flexible_spec', 'targeting.excluded_geo_locations.regions', 'targeting.geo_locations.cities', 'targeting.excluded_geo_locations.cities', 'targeting.geo_locations.regions', 'targeting.geo_locations.geo_markets', 'targeting.app_install_state', 'targeting.user_device', 'targeting.user_os', 'targeting.excluded_custom_audiences', 'targeting.excluded_geo_locations.geo_markets', 'targeting.excluded_product_audience_specs', 'targeting.product_audience_specs'], index=False, sep=',', encoding='utf-8')  # TRIM UNNECESSARY ADSET TARGETING METADATA AND SAVE AS CSV
numacct = ind_a + 1
sys.stdout.write(f'\nDownloaded total of {total_num_adsets} adsets across {numacct} accounts in business account #{business_account}.\n\n')
# END DOWNLOADING ALL AD SET TARGETING METADATA
