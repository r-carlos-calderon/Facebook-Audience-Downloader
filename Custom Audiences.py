import requests
import datetime
import pandas as pd
import json
import sys
import os
from pandas import json_normalize

# THIS SCRIPT DOWNLOADS METADATA AS TWO FILES FOR THE FOLLOWING: AUDIENCES AND TARGETING CRITERIA OF ADSETS

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

# BEGIN DOWNLOADING ALL AUDIENCE METADATA
all_audiences_json = []
total_num_audiences = 0
for ind_a in df_acct.index:  # ITERATE THROUGH ALL AD ACCOUNTS FROM STEP 1 TO DISCOVER ALL AUDIENCES (STEP 3)
    acct = str(df_acct['account_id'][ind_a])
    res = requests.get(
         f'https://graph.facebook.com/v7.0/act_{acct}/customaudiences?fields=id&limit=9999&pretty=1&access_token={token}&export_format=csv')
    r1 = res.text
    r2 = json.loads(r1)
    r3 = json.dumps(r2['data'])
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
