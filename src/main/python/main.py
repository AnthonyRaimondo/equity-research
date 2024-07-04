import datetime
import re
import time
from pathlib import Path

import pandas as pd
import requests

import mongo_client
from constant.dates import months

cik_url = 'https://www.sec.gov/Archives/edgar/cik-lookup-data.txt'
NEW_ONLY = True
RESOURCES_PATH = Path()
TODAY = datetime.datetime.today()
MONTH = TODAY.month
YEAR = TODAY.year


def save_cik(cik_number: str, filename: str) -> None:
    file_path = RESOURCES_PATH / 'monthly_cik_numbers' / str(YEAR) / months.get(MONTH) / f'{filename}.csv'
    try:
        existing_cik_numbers = pd.read_csv(file_path)
        existing_cik_list = existing_cik_numbers.loc[:, 0].to_list()
        existing_cik_list.append(cik_number)
        updated_cik_numbers = list(set(existing_cik_numbers))
        pd.DataFrame(updated_cik_numbers).to_csv(file_path, index=False)
    except FileNotFoundError:  # Create a file if it doesn't already exist
        pd.DataFrame([cik_number]).to_csv(file_path, index=False)


if __name__ == "__main__":
    headers = {
        "User-Agent": "Anthony Raimondo anthonyraimondo7@gmail.com",
        "Accept-Encoding": "gzip,deflate",
    }

    # retrieve all cik numbers
    cik_response = requests.get(cik_url, headers=headers)
    cik_numbers = list(set([
        re.findall(r"\:\s*\+?(-?\d+)\s*\:", entry)[0]
        for entry in cik_response.text.split('\n') if entry
    ]))
    if NEW_ONLY:
        existing_ciks = mongo_client.get_unique_ids(
            db_name='finance', collection_name='submission_meta_data', query='cik'
        )
        cik_numbers = [num for num in cik_numbers if num not in existing_ciks]

    # get submission metadata for all cik numbers
    for cik in cik_numbers:
        url = f'https://data.sec.gov/submissions/CIK{cik}.json'
        try:
            response = requests.get(url, headers=headers)
        except requests.exceptions.ConnectionError:
            print('Connection timeout - sleeping for two minutes')
            time.sleep(120)
            continue
        try:
            submission_json = response.json()
        except requests.exceptions.JSONDecodeError:
            save_cik(cik, filename='exception')
            continue

        # don't care about submissions for entities that have no ticker or exchange
        if submission_json.get('tickers') or submission_json.get('exchanges'):

            # check to see if all data in this submission is already saved down
            document = mongo_client.read(
                db_name='finance',
                collection_name='submission_meta_data',
                query={'cik': cik},
            )
            if document is not None:
                latest_submission_list = list(
                    sorted(submission_json.get('filings').get('recent').get('accessionNumber'))
                )
                saved_submission_list = list(
                    sorted(document.get('filings').get('recent').get('accessionNumber'))
                )
                if latest_submission_list == saved_submission_list:
                    save_cik(cik, filename='no-new-filings')
                    continue  # no new submissions

            # submission metadata is new, or has been updated
            print(f'saving cik {cik}, ticker {submission_json.get("tickers")[0]}')
            submission_json['cik'] = cik
            mongo_client.write(
                db_name='finance',
                collection_name='submission_meta_data',
                document=submission_json,
                insert_one=True,
            )
            save_cik(cik, filename='new-filings')

        else:
            save_cik(cik, filename='no-ticker')

        # max allowable requests per second = 10
        time.sleep(0.12)
