import requests
import numpy as np
import pandas as pd
import json
import sqlite3

from secrets import config


base_vote_url = 'https://api.propublica.org/congress/v1/{chamber}/\
votes/{year}/{month}.json'

for year in np.arange(1989,2019):
    for month in np.arange(1,13):
        url = base_vote_url.format(chamber='senate',
                                   year=year,
                                   month=month)
        get_and_save_votes(url,full_results)

full_results = []
def get_and_save_votes(url,full_results):
    r = requests.get(url, headers=config.api_params)
    full_results = full_results.append(r)

def rep_to_json(resp,fail_log):
    try:
        return resp.json()['results']['votes']
    except:
        try:
            resp = resp.text.replace('\n','').replace('\r','').replace(": ,",""":"",""").replace(':             }',""":""}""")
            d = json.loads(resp)
            return d
        except:
            print('\t-- -- --Loads Failed')
        fail_log = fail_log.append(resp)
        return None

fail_log =[]
hope_json = [rep_to_json(resp,fail_log) for resp in full_results]

final_df = pd.DataFrame()
for i, segment in enumerate(hope_json):
    df = pd.DataFrame(segment, index=range(len(segment)))
    final_df = final_df.append(df)

null_dict = {'bill_id': '-101',
	'number': '',
	'sponsor_id': None,
	'api_uri': None,
	'title': None,
	'latest_action': None}

final_df['bill'] = final_df['bill'].fillna('')
final_df['bill'] = np.where(final_df['bill']=={},null_dict,final_df['bill'])

def verify_bill_keys(bill):
    if type(bill) == dict:
        bill = bill
    else:
        bill = {}
    try:
        bill['bill_id'] = bill['bill_id']
    except:
        bill['bill_id'] = '-101'
    try:
        bill['number'] = bill['number']
    except:
        bill['number'] = ''
    try:
        bill['sponsor_id'] = bill['sponsor_id']
    except:
        bill['sponsor_id'] = None
    try:
        bill['api_uri'] = bill['api_uri']
    except:
        bill['api_uri'] = None
    try:
        bill['title'] = bill['title']
    except:
        bill['title'] = None
    try:
        bill['latest_action'] = bill['latest_action']
    except:
        bill['latest_action'] = None
    return bill

final_df['bill'] = final_df['bill'].map(verify_bill_keys)

final_df[['api_uri', 'bill_id', 'latest_action',
          'number', 'sponsor_id', 'title']] = final_df['bill'].apply(pd.Series)

final_df['democratic'] = final_df['democratic'].fillna('')
final_df['democratic'] = np.where(final_df['democratic']=={},null_dict,final_df['democratic'])

def verify_party_keys(party):
    if type(party) == dict:
        party = party
    else:
        party = {}
    try:
        party['yes'] = party['yes']
    except:
        party['yes'] = None
    try:
        party['no'] = party['no']
    except:
        party['no'] = None
    try:
        party['present'] = party['present']
    except:
        party['present'] = None
    try:
        party['not_voting'] = party['not_voting']
    except:
        party['not_voting'] = None
    try:
        party['majority_position'] = party['majority_position']
    except:
        party['majority_position'] = None
    return party

final_df['democratic'] = final_df['democratic'].map(verify_party_keys)

final_df[['majority_position', 'no', 'not_voting',
          'present', 'yes']] = final_df['democratic'].apply(pd.Series)
final_df = final_df.rename(columns={'yes':'democratic_yes',
                        'no':'democratic_no',
                        'present':'democratic_present',
                        'not_voting':'democratic_not_voting',
                        'majority_position':'democratic_majority_position'})

final_df['republican'] = final_df['republican'].fillna('')
final_df['republican'] = np.where(final_df['republican']=={},null_dict,final_df['republican'])
##
final_df['republican'] = final_df['republican'].map(verify_party_keys)

final_df[['majority_position', 'no', 'not_voting',
          'present', 'yes']] = final_df['republican'].apply(pd.Series)
final_df = final_df.rename(columns={'yes':'republican_yes',
                        'no':'republican_no',
                        'present':'republican_present',
                        'not_voting':'republican_not_voting',
                        'majority_position':'republican_majority_position'})

final_df['independent'] = final_df['independent'].fillna('')
final_df['independent'] = np.where(final_df['independent']=={},null_dict,final_df['independent'])
##
final_df['independent'] = final_df['independent'].map(verify_party_keys)

final_df[['majority_position', 'no', 'not_voting',
          'present', 'yes']] = final_df['independent'].apply(pd.Series)
final_df = final_df.rename(columns={'yes':'independent_yes',
                        'no':'independent_no',
                        'present':'independent_present',
                        'not_voting':'independent_not_voting',
                        'majority_position':'independent_majority_position'})

final_df['total'] = final_df['total'].fillna('')
final_df['total'] = np.where(final_df['total']=={},null_dict,final_df['total'])
##
final_df['total'] = final_df['total'].map(verify_party_keys)

final_df[['majority_position', 'no', 'not_voting',
          'present', 'yes']] = final_df['total'].apply(pd.Series)
final_df = final_df.rename(columns={'yes':'total_yes',
                        'no':'total_no',
                        'present':'total_present',
                        'not_voting':'total_not_voting',
                        'majority_position':'total_majority_position'})

del final_df['bill']
del final_df['democratic']
del final_df['republican']
del final_df['independent']
del final_df['total']

conn = sqlite3.connect(config.db_params['database'])

final_df.to_sql(config.db_params['table_overview'], conn, if_exists='replace')

### Specific Votes

base_vote_url = 'https://api.propublica.org/congress/v1/{congress}/\
{chamber}/sessions/{session}/votes/{roll_number}.json'

url = base_vote_url.format(congress='80',
                              chamber='senate',
                              session='1',
                              roll_number='200')

for_vote_call = final_df.groupby(['congress','chamber','session','roll_call']).sum()[[]].reset_index()
for_vote_call['chamber'] = 'senate'
for_vote_call['congress'] = for_vote_call['congress'].astype(int)
for_vote_call['session'] = for_vote_call['session'].astype(int)
for_vote_call['roll_call'] = for_vote_call['roll_call'].astype(int)

def request_vote_info(vote_params):
    base_vote_url = 'https://api.propublica.org/congress/v1/{congress}/{chamber}/sessions/{session}/votes/{roll_call}.json'
    url = base_vote_url.format(congress=vote_params['congress'],
                              chamber='senate',
                              session=vote_params['session'],
                              roll_call=vote_params['roll_call'])
    r = requests.get(url, headers=config.api_params)
    try:
        if r.json()['message'] == 'Endpoint request timed out':
            r = requests.get(url, headers=config.api_params, timeout=10)
        elif r.json()['status'] == 'Internal Server Error':
            r = requests.get(url, headers=config.api_params, timeout=10)
        else:
            return r
    except:
        return r

# check for continuity
for x in range(len(for_vote_call)):
    for_vote_call.loc[x]

votes = []
for x in range(10000):
    try:
        votes = votes + [for_vote_call.loc[x].to_dict()]
    except:
        pass

from multiprocessing.pool import Pool

with Pool(4) as p:
    results = p.map(request_vote_info, votes)

def get_individula_vote_results(full_result):
    try:
        return full_result.json()['results']['votes']
    except:
        try:
            print(full_result.json()['message'])
            return None
        except:
            print(full_result.text)
            return None

result_list = [get_individula_vote_results(result) for result in results]
result_list = list(filter(None.__ne__, result_list))
vacant_seats = pd.DataFrame(result_list)[['vacant_seats']]
result_list2 = [result['vote'] for result in result_list]

vote_df = pd.DataFrame(result_list2)

## Flat columns
vote_df = vote_df.join(pd.DataFrame(vote_df['bill'].tolist()))
vote_df = vote_df.join(pd.DataFrame(vote_df['democratic'].tolist()), rsuffix='_democratic')
vote_df = vote_df.join(pd.DataFrame(vote_df['independent'].tolist()), rsuffix='_independent')
vote_df = vote_df.join(pd.DataFrame(vote_df['republican'].tolist()), rsuffix='_republican')
vote_df = vote_df.join(pd.DataFrame(vote_df['total'].tolist()), rsuffix='_total')

positions = vote_df.groupby(['congress','session','roll_call']).first()[['positions']].reset_index()

pos_final = pd.DataFrame()
for i, row in enumerate(positions['positions']):
    cong, sess, roll = positions.loc[i,'congress'], positions.loc[i,'session'], positions.loc[i,'roll_call']
    pos_row = pd.DataFrame(row, index=np.arange(len(row)))
    pos_row['congress'] = cong
    pos_row['session'] = sess
    pos_row['roll_call'] = roll
    pos_final = pd.concat([pos_final,pos_row])
    if i % 1000 == 0:
        pos_final.to_csv('{}/positions_{}.csv'.format(config.temp_storage,i))
        print(i)
        pos_final = pd.DataFrame()

print(len(pos_final))
for csv in ['positions_0.csv','positions_1000.csv','positions_2000.csv','positions_3000.csv','positions_4000.csv']:
    try:
        newest_csv = pd.read_csv('{}/{}'.format(config.temp_storage,csv), engine='python')
        print(len(newest_csv))
        pos_final = pos_final.append(newest_csv)
    except:
        pass
print('final length{}'.format(len(pos_final)))
del pos_final['Unnamed: 0']

conn = sqlite3.connect(config.db_params['database'])

pos_final.to_sql(config.db_params['table_individual_votes'], conn, if_exists='append')