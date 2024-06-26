import re
import time

import requests

import mongo_client

cik_url = 'https://www.sec.gov/Archives/edgar/cik-lookup-data.txt'
NEW_ONLY = True

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
        response = requests.get(url, headers=headers)
        try:
            submission_json = response.json()
        except requests.exceptions.JSONDecodeError:
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
                a = 1
                latest_submission_list = list(
                    sorted(submission_json.get('filings').get('recent').get('accessionNumber'))
                )
                saved_submission_list = list(
                    sorted(document.get('filings').get('recent').get('accessionNumber'))
                )
                if latest_submission_list == saved_submission_list:
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

        # max allowable requests per second = 10
        time.sleep(0.12)
