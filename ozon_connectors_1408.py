#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import pandas as pd
from google.oauth2 import service_account
import datetime as dt
from datetime import timedelta

if __name__ == '__main__':
    credentials = service_account.Credentials.from_service_account_file(
    '/Users/jullia/Downloads/dogeat-321311-0d748e5d5c69.json'
    )
    
## costs 
#     json={
#     "date_from": start_date,
#     "date_to": end_date,
#     "metrics": [
#         "adv_sum_all"
#     ],
#     "dimension": [
#         "day",
#         "month",
#         "year"
#     ],
#     "filters": [],
#     "sort": [
#     ],
#     "limit": 1000,
#     "offset": 0
#     }

# ## compensation
#     json_2={
#             "date": {
#             "from": date,
#             "to": date
#             },
#             "posting_number": "",
#             "transaction_type": "all"
#             }
# headers={
#     'Client-Id':'42918', 
#     'Api-Key':'edbfc6b6-e0fc-446d-9c5d-454acf372925'
#     } 

## returns dataframes ready to bigquery 
    headers={
        'Client-Id':'42918', 
        'Api-Key':'edbfc6b6-e0fc-446d-9c5d-454acf372925'
    } 

    def get_data(json,headers,url):
        req = requests.post(url, headers=headers, json=json)
        if req.status_code == 200:
            return req.json()
        else:
            raise ValueError

    def get_costs(date):
        json={
     "date_from": date,
     "date_to": date,
     "metrics": [
     "adv_sum_all"
     ],
   "dimension": [
     "day",
       "month",
      "year"
   ],
     "filters": [],
     "sort": [
   ],
    "limit": 1000,
    "offset": 0
    }
        url = 'https://api-seller.ozon.ru/v1/analytics/data'
        j = get_data(json, headers, url)
        df_cost = pd.DataFrame.from_dict(j) 
        key = df_cost['result']['data'][0]['dimensions'][0]['id']
        value = df_cost['result']['totals'][0]
        data_cost = pd.DataFrame({'date':[key],'source':['partner.ozon.ru'],'medium':['referral'],'campaign':[" "],'Cost':[value]})
        return data_cost

    def get_compensation(date):
        json={
             "date": {
           "from": date + 'T00:00:00.000Z',
            "to": date + 'T00:00:00.000Z'
            },
            "posting_number": "",
             "transaction_type": "all"
           }
        url = 'https://api-seller.ozon.ru/v3/finance/transaction/totals'
        j = get_data(json, headers, url)
        data_comp =    pd.DataFrame.from_dict(j).T.reset_index(drop=True)
        data_comp['Cost'] = abs(data_comp.drop('accruals_for_sale', axis=1).sum(axis=1))
        data_comp['date'] = date
        data_comp['source'] = 'partner.ozon.ru'
        data_comp['medium'] = 'referral'
        return data_comp


    def date_range_datetime(start, end):
        delta = end - start
        days_dt  = [start + datetime.timedelta(days=i) for i in range(delta.days + 1)]
        return days_dt


    def date_range_string(start, end):
        start  = datetime.strptime(start, '%Y-%m-%d').date()
        end = datetime.strptime(end, '%Y-%m-%d').date()
        date_range_datetime(start, end)

    def data_to_gbq(start, end):

        for day in date_range_string(start,end):
            
            df1 = get_costs(day)
            df2 = get_compensation(day)

            df1.to_gbq('Costs.HOLOWAY_OZON_Costs_compensation',
                ##chunksize=None, # I have tried with several chunk sizes, it runs faster when it's one big chunk (at least for me)
                if_exists='append',
                credentials=credentials,
                chunksize=None,
                table_schema=None
                )

            df2.to_gbq('Costs.HOLOWAY_OZON_Costs',
                        ##chunksize=None, # I have tried with several chunk sizes, it runs faster when it's one big chunk (at least for me)
                        if_exists='append',
                        credentials=credentials,
                        chunksize=None,
                        table_schema=None
                )


