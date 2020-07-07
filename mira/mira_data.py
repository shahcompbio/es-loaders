
from paramiko import SSHClient
from scp import SCPClient
import logging
import os
import sys
import json


import mira.constants as constants
import pickle
import collections
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SAMPLE_SPREADSHEET_ID = '1plhIL1rH2IuQ8b_komjAUHKKrnYPNDyhvNNRsTv74u8'
SAMPLE_RANGE_NAME = 'sample_metadata'

SORT_ENCODER = {'singlet, live, CD45+': 'CD45P',
                'singlet, live, CD45-': 'CD45N', 'singlet, live, U': 'U'}


def download_metadata(analyses, base_directory):
    metadata = get_metadata()
    for analysis in analyses:
        directory = os.path.join(base_directory, analysis["dashboard_id"])

        generate_metadata_json(analysis, metadata, directory)
        print(f'Done download for {analysis["dashboard_id"]}')

def download_analyses_data(type, analyses, base_directory):

    metadata = get_metadata()

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('juno')

    with SCPClient(ssh.get_transport(), progress=progress) as scp:

        for analysis in analyses:
            directory = os.path.join(base_directory, analysis["patient_id"])
            if not os.path.exists(directory):
                os.makedirs(directory)

            scp.get(os.path.join(analysis["juno_storage"], constants.CELLS_FILENAME), directory)
            scp.get(os.path.join(analysis["juno_storage"], constants.GENES_FILENAME), directory)
            scp.get(os.path.join(analysis["juno_storage"], constants.MATRIX_FILENAME), directory)
            scp.get(os.path.join(analysis["juno_storage"], constants.JUNO_SAMPLES_FILENAME), directory)


            generate_metadata_json(analysis, metadata, directory)
            print(f'Done download for {analysis["patient_id"]}')

def progress(filename, size, sent):
    sys.stdout.write("%s\'s progress: %.2f%%   \r" % (filename, float(sent)/float(size)*100) )


def get_metadata():        
    data = open_file()
    header = data.pop(0)

    df = pd.DataFrame.from_records(
        [dict(zip(header, row)) for row in data])

    df['sort_parameters'] = df['sort_parameters'].map(
        SORT_ENCODER)

    data = df.to_dict('records')
    sample_ids = [row["isabl_id"] for row in data]

    return dict(zip(sample_ids, data))



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

    service = build('sheets', 'v4', credentials=creds,
                    cache_discovery=False)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values', [])

    return values

def generate_metadata_json(analysis, metadata, directory):
    samples = pd.read_csv(os.path.join(directory, constants.JUNO_SAMPLES_FILENAME), sep='\t')
    samples = samples[["sample_id"]].values.tolist()
    samples = [sample[0] for sample in samples]

    sample_data = [metadata[sample_id] for sample_id in samples]
    processed_metadata = [_transform_metadata(sample, analysis["dashboard_id"]) for sample in sample_data]

    metadata_path = os.path.join(directory, constants.SAMPLES_FILENAME)

    if os.path.exists(metadata_path):
        os.remove(metadata_path)

    with open(os.path.join(directory, constants.SAMPLES_FILENAME), 'w+') as outfile:
        json.dump(processed_metadata, outfile)

def _transform_metadata(sample, dashboard_id):
    return {
        "sample_id": sample["isabl_id"],
        "patient_id": sample["patient_id"],
        "dashboard_id": dashboard_id,
        "site": sample["tumor_subsite"],
        "tumor_type": sample["tumor_type"],
        "sort": sample["sort_parameters"],
        "therapy": sample["therapy"],
        "surgery": sample["surgery"]
    }


def download_cohort_data(cohort_analysis, cohort_celltype_analyses, directory):
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('juno')

    with SCPClient(ssh.get_transport(), progress=progress) as scp:
        # scp.get(os.path.join(cohort_analysis["juno_storage"], constants.CELLS_FILENAME), directory)
        # scp.get(os.path.join(cohort_analysis["juno_storage"], constants.GENES_FILENAME), directory)
        # scp.get(os.path.join(cohort_analysis["juno_storage"], constants.MATRIX_FILENAME), directory)

        for analysis in cohort_celltype_analyses:
            scp.get(analysis["juno_storage"], os.path.join(directory, analysis["dashboard_id"]+"_cells.tsv"))

            print(f'Done download for {analysis["dashboard_id"]}')


    metadata = get_metadata()
    sample_list = [_transform_metadata(metadata[sample_id], cohort_analysis["dashboard_id"]) for sample_id in dict(metadata)]

    with open(os.path.join(directory, constants.SAMPLES_FILENAME), 'w+') as outfile:
        json.dump(sample_list, outfile)


def get_celltype_analyses(cohort_analysis):
    ## Just hardcoding here
    cell_types = ["B.cell","B.super", "Dendritic.cell", "Endothelial.cell", "Fibroblast", "Monocyte", "Myeloid.super", "Ovarian.cancer.cell", "Ovarian.cancer.super", "Plasma.cell", "Stromal.super", "Mast.cell", "T.cell", "T.super"]
    return [{
        "juno_storage": cohort_analysis["juno_storage"] + "/celltypes/" + cell_type + "_cells.tsv",
        "modified": cohort_analysis["modified"],
        "dashboard_id": "cohort_" + cell_type.replace(".", "-").lower()
    } for cell_type in cell_types]



# download_analyses_data([{'pk': 1651, 'modified': '2020-05-20T17:46:52.921614-04:00', 'juno_storage': '/work/shah/isabl_data_lake/analyses/16/51/1651', 'patient_id': 'SPECTRUM-OV-031'}, {'pk': 1659, 'modified': '2020-05-20T17:46:15.225621-04:00', 'juno_storage': '/work/shah/isabl_data_lake/analyses/16/59/1659', 'patient_id': 'SPECTRUM-OV-002'}, {'pk': 1688, 'modified': '2020-05-21T00:31:30.208087-04:00', 'juno_storage': '/work/shah/isabl_data_lake/analyses/16/88/1688', 'patient_id': 'SPECTRUM-OV-007'}, {'pk': 1702, 'modified': '2020-05-20T22:11:20.254439-04:00', 'juno_storage': '/work/shah/isabl_data_lake/analyses/17/02/1702', 'patient_id': 'SPECTRUM-OV-003'}], '/data/mira')