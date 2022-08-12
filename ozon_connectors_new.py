#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests
import json
import pandas as pd
import pandas_gbq
import google.auth
from google.oauth2 import service_account
from datetime import datetime, timedelta
import sys
import getopt
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine 

if __name__ == '__main__':
    credentials = service_account.Credentials.from_service_account_file(
    '/Users/jullia/Downloads/dogeat-321311-0d748e5d5c69.json'
    )
    
## costs 
#     json_1={
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

    def get_data(date,json,headers,url):
        if 'date_from' in json:
            json["date_from"] = date
        elif 'from' in json['date']:
            json['date']['from'] = date + 'T00:00:00.000Z'

        req = requests.post(url, headers=headers, json=json)
        if req.status_code == 200:
            return req.json()
        else:
            raise ValueError

    def get_costs(params):
        j = get_data(params)
        df_cost = pd.DataFrame.from_dict(j) 
        key = df_cost['result']['data'][0]['dimensions'][0]['id']
        value = df_cost['result']['totals'][0]
        data_cost = pd.DataFrame({'date':[key],'source':['partner.ozon.ru'],'medium':['referral'],'campaign':[" "],'Cost':[value]})
        return data_cost

    def get_compensation(params):
        j = get_data(params)
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

    def data_to_gbq(start, end, params):

        for day in date_range_string(start,end):
            
            df1 = get_costs(params)
            df2 = get_compensation(params)

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


