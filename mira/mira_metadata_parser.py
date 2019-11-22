import pickle
import os
import collections
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import csv

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SAMPLE_SPREADSHEET_ID = '1plhIL1rH2IuQ8b_komjAUHKKrnYPNDyhvNNRsTv74u8'
SAMPLE_RANGE_NAME = 'sample_metadata!A1:L'


def open_file():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values', [])

    return values


def all_samples():
    #values = open_file()
    with open('metadata.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        samples = collections.defaultdict(dict)

        #header = values.pop(0)
        header = next(csv_reader, None)
        for row in csv_reader:
            sample = dict(zip(header, row))
            samples[sample["nick_unique_id"]] = sample
        return samples


def patient_samples(patient_id, sheet=None):
    samples = sheet if sheet is not None else all_samples()
    valid_samples = []
    for unique_id, fields in samples.items():
        if fields["patient_id"] == patient_id:
            valid_samples.append(fields)
    return valid_samples


def single_sample(nick_unique_id, sheet=None):
    samples = sheet if sheet is not None else all_samples()
    return samples[nick_unique_id]


def all_sample_ids():
    return [sample_id
            for sample_id, fields in all_samples().items()]


def all_patient_sort_ids():
    samples = [fields for sample_id, fields in all_samples().items()]
    df = pd.DataFrame.from_records(samples)

    patient_df = df[['patient_id', 'sort_parameters']].drop_duplicates()
    patient_df['sort_parameters'] = patient_df['sort_parameters'].map(
        {'singlet, live, CD45+': 'CD45P', 'singlet, live, CD45-': 'CD45N', 'singlet, live, U': 'U'})

    return [row[0] + "_" + row[1] for row in df.values.tolist()]


if __name__ == '__main__':
    all_samples = all_samples()
    print(all_samples)
