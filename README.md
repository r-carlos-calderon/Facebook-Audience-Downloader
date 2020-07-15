# Facebook Audiences Downloader
At the time of this writing, Facebook Ads Manager lacks the ability to export audience lists from Ad Accounts. This python package uses [Facebook's REST-API](https://thd.co/2CAyfLp) to read and download audience lists from ad accounts under a single business account.

## Why use this tool
* If you need to export audience lists for auditing/monitoring/troubleshooting
* If you need to read additional metadata about audiences not available in the UI
* If you need to identify which audiences are being used in advertising
* If you need to export targeting criteria of ad sets

## What it does
* The [Audience Downloader.py](https://github.com/r-carlos-calderon/Facebook-Audience-Downloader/blob/master/Audience%20Downloader.py) reads and downloads all audiences from ad accounts within business manager both as a nested json and normalized csv files. It also generates log files in both text and csv files. The [Audience Downloader Light.py](https://github.com/r-carlos-calderon/Facebook-Audience-Downloader/blob/master/Audience%20Downloader%20Light.py) is less "bloated" and generates a single nested json file.
* The [Targeting Downloader.py](https://github.com/r-carlos-calderon/Facebook-Audience-Downloader/blob/master/Targeting%20Downloader.py) reads and downloads Ad Set metadata from ad accounts within business manager both as a nested json and normalized csv files. It also generates log files in both text and csv files. The [Targeting Downloader Light.py](https://github.com/r-carlos-calderon/Facebook-Audience-Downloader/blob/master/Targeting%20Downloader%20Light.py) is less "bloated" and generates a single nested json file.

### How to use 

- **Python 3.8:** This package was authored using [Python 3.8](https://www.python.org/downloads/release/python-380/) interpreter and [PyCharm 2020.1.3](https://www.jetbrains.com/pycharm/download/other.html).
- **Config File:** Create a `config.json` file containing the following:

  ```
  {
	    "token": "<SYSTEM USER TOKEN>",
	    "business_account": 1234567890,
	    "cust_fields": "account_id,id,name,description,approximate_count,retention_days,customer_file_source,subtype,time_created,time_updated,data_source,permission_for_actions,delivery_status,operation_status,lookalike_audience_ids",
	    "adset_fields": "account_id,campaign_id,id,targeting,status,effective_status,configured_status",
	    "cust_limit": 500,
	    "adset_limit": 500
  }
  ```
   - **Token:** [Add a System User to Your Business Manager](https://www.facebook.com/business/help/503306463479099) and generate a new token with _ads\_management_, _read\_insights_, and _business\_management_ permissions. (Note: Don't forget to give the system user access to ad accounts.)
   - **Business Account:** This id can be found under [Business Manager Info](https://business.facebook.com/settings/info).
   - **cust_fields:** See developer documentation for a list of [Custom Audience Fields](https://developers.facebook.com/docs/marketing-api/reference/custom-audience#fields).
   - **adset_fields:** See developer documentation for a list of [Ad Set Fields](https://developers.facebook.com/docs/marketing-api/reference/ad-campaign#fields).
   - **_limits:** The string of field names determine the detail and density of data requested and may return the following error. Limit values may be any integer between 25 - 5000.
   
  ```
  {
    "error": {
        "code": 1,
        "message": "Please reduce the amount of data you're asking for, then retry your request"
    }
  } 
  ```