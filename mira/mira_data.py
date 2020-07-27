
from paramiko import SSHClient
from scp import SCPClient
import logging
import os
import sys
import json
import requests
import io
import pickle
import collections
import pandas as pd
import numpy as np

import mira.constants as constants
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

logger = logging.getLogger('mira_loading')

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


def download_analyses_data(type, analyses, base_directory, cohort_group=None):

    metadata = get_metadata()

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect('juno')

    with SCPClient(ssh.get_transport(), progress=progress) as scp:

        if type == "patient":
            for analysis in analyses:
                directory = os.path.join(base_directory, analysis["dashboard_id"])
                logger.info(f'Starting download for {analysis["dashboard_id"]}')
                if not os.path.exists(directory):
                    logger.info(f'Creating directory: {directory}')
                    os.makedirs(directory)


                logger.info(f'{analysis["dashboard_id"]}: Downloading cells')
                scp.get(os.path.join(analysis["juno_storage"], constants.CELLS_FILENAME), directory)
                logger.info(f'{analysis["dashboard_id"]}: Downloading genes')
                scp.get(os.path.join(analysis["juno_storage"], constants.GENES_FILENAME), directory)
                logger.info(f'{analysis["dashboard_id"]}: Downloading matrix')
                scp.get(os.path.join(analysis["juno_storage"], constants.MATRIX_FILENAME), directory)
                logger.info(f'{analysis["dashboard_id"]}: Downloading samples')
                scp.get(os.path.join(analysis["juno_storage"], constants.JUNO_SAMPLES_FILENAME), directory)

                generate_metadata_json(analysis, metadata, directory)
                generate_marker_genes(analysis, directory)
                logger.info(f'Done download for {analysis["dashboard_id"]}')

        elif type == "cohort":
            if cohort_group == "cohort":
                cohort_analysis = analyses[0]
                subset_analyses = []
            elif cohort_group == "cell_type":
                cohort_analysis = None
                subset_analyses = analyses
            elif cohort_group == "both":
                cohort_analysis = analyses[0]
                subset_analyses = analyses[1:]
            
            if cohort_analysis is not None:
                directory = os.path.join(base_directory, cohort_analysis["dashboard_id"])
                logger.info(f'Starting download for {cohort_analysis["dashboard_id"]}')
                if not os.path.exists(directory):
                    logger.info(f'Creating directory: {directory}')
                    os.makedirs(directory)

                ## download main cohort file
                logger.info(f'{cohort_analysis["dashboard_id"]}: Downloading cells')
                scp.get(os.path.join(cohort_analysis["juno_storage"], constants.CELLS_FILENAME), directory)
                logger.info(f'{cohort_analysis["dashboard_id"]}: Downloading genes')
                scp.get(os.path.join(cohort_analysis["juno_storage"], constants.MATRIX_FILENAME), os.path.join(directory, constants.GENES_FILENAME))
                ## it's always misnamed here
                logger.info(f'{cohort_analysis["dashboard_id"]}: Downloading matrix')
                scp.get(os.path.join(cohort_analysis["juno_storage"], constants.GENES_FILENAME), os.path.join(directory, constants.MATRIX_FILENAME))

                generate_cohort_metadata_json(cohort_analysis, metadata, directory)
                generate_marker_genes(cohort_analysis, directory)
                logger.info(f'Done download for {cohort_analysis["dashboard_id"]}')

            if len(subset_analyses) > 0:
                ## assume that cohort data is already downloaded
                cohort_directory = os.path.join(base_directory, "cohort_all")
                 
                for analysis in subset_analyses:
                    directory = os.path.join(base_directory, analysis["dashboard_id"])
                    logger.info(f'Starting download for {analysis["dashboard_id"]}')
                    if not os.path.exists(directory):
                        logger.info(f'Creating directory: {directory}')
                        os.makedirs(directory)
                    ## transfer the cell type file over and rename it appropriately
                    ## sym link to the large cohort genes and matrix file
                    logger.info(f'{analysis["dashboard_id"]}: Downloading cells')
                    scp.get(analysis["juno_storage"] + "_embedding.tsv", os.path.join(directory, constants.CELLS_FILENAME))
                    reprocess_cohort_subset(directory, cohort_directory, analysis["dashboard_id"])

                    logger.info(f'{analysis["dashboard_id"]}: Linking genes')
                    os.symlink(os.path.join(cohort_directory, constants.GENES_FILENAME), os.path.join(directory, constants.GENES_FILENAME))
                    logger.info(f'{analysis["dashboard_id"]}: Linking matrix')
                    os.symlink(os.path.join(cohort_directory, constants.MATRIX_FILENAME), os.path.join(directory, constants.MATRIX_FILENAME))

                    generate_cohort_metadata_json(analysis, metadata, directory)

                    ## marker gene matrix is different
                    scp.get(analysis["juno_storage"] + "_marker_sheet.tsv", os.path.join(directory, constants.JUNO_MARKERS_FILENAME))
                    generate_cohort_subset_marker_genes(analysis, directory)
                    # generate_marker_genes(analysis, directory)
                    logger.info(f'Done download for {analysis["dashboard_id"]}')

            


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
        logger.info("Refreshing metadata")
        os.remove(metadata_path)

    logger.info(f'{analysis["dashboard_id"]}: Create metadata with {len(processed_metadata)} samples')
    with open(os.path.join(directory, constants.SAMPLES_FILENAME), 'w+') as outfile:
        json.dump(processed_metadata, outfile)


def generate_cohort_metadata_json(analysis, metadata, directory):
    processed_metadata = [_transform_metadata(metadata[sample_id], analysis["dashboard_id"]) for sample_id in dict(metadata)]
    
    metadata_path = os.path.join(directory, constants.SAMPLES_FILENAME)

    if os.path.exists(metadata_path):
        logger.info("Refreshing metadata")
        os.remove(metadata_path)

    logger.info(f'{analysis["dashboard_id"]}: Create metadata with {len(processed_metadata)} samples')
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


def generate_marker_genes(analysis, directory):
    git_session = requests.Session()
    git_session.auth = (os.environ["GITHUB_USER"], os.environ["GITHUB_ACCESS_TOKEN"])

    download = git_session.get(constants.MARKER_GENES_URL).content

    marker_genes = pd.read_csv(io.StringIO(download.decode('utf-8')))
    marker_genes = marker_genes.transpose()

    gene_list = marker_genes[:'Unnamed: 0'].values.tolist()[0]
    marker_genes.columns = gene_list
    marker_genes = marker_genes.iloc[1:]

    marker_genes.index.name = 'cell_type'
    marker_genes = marker_genes.reset_index(drop=False)
    marker_genes['cell_type'] = marker_genes['cell_type'].str.replace('.', ' ')
    cell_types = marker_genes['cell_type'].values

    data = []
    for i in range(len(cell_types)):
        row, cols = np.where(marker_genes[i:i+1].values == 1)

        record = {
            "cell_type": cell_types[i],
            "genes": list(marker_genes.columns[cols])
        }

        data.append(record)

    logger.info(f'{analysis["dashboard_id"]}: Create marker gene files with {len(data)} cells')
    with open(os.path.join(directory, constants.MARKER_GENES_FILENAME), 'w+') as outfile:
        json.dump(data, outfile)

def generate_cohort_subset_marker_genes(analysis, directory):
    marker_genes = pd.read_csv(os.path.join(directory, constants.JUNO_MARKERS_FILENAME), sep='\t')

    cell_types = [col for col in list(marker_genes.columns) if col.lower() != 'rank']

    data = []
    for i in range(len(cell_types)):
        record = {
            "cell_type": cell_types[i].replace('.', ' '),
            "genes": [x for x in list(marker_genes[cell_types[i]]) if str(x) != 'nan']
        }

        data.append(record)

    logger.info(f'{analysis["dashboard_id"]}: Create marker gene files with {len(data)} cells')
    with open(os.path.join(directory, constants.MARKER_GENES_FILENAME), 'w+') as outfile:
        json.dump(data, outfile)

    


def get_celltype_analyses(cohort_analysis):
    ## Just hardcoding here
    ## We will be grabbing these in Florian's directory
    cell_types = ["B.super", "Endothelial.cell", "Fibroblast", "Myeloid.super", "T.cell"]
    return [{
        "juno_storage": "/work/shah/uhlitzf/data/SPECTRUM/freeze/v5/" + cell_type,
        "modified": cohort_analysis["modified"],
        "dashboard_id": "cohort_" + cell_type.replace(".", "-").lower()
    } for cell_type in cell_types]



def reprocess_cohort_subset(directory, cohort_directory, dashboard_id): 
    cells_filename = os.path.join(directory, constants.CELLS_FILENAME)
    
    cohort_cells_filename = os.path.join(cohort_directory, constants.CELLS_FILENAME)

    ## need to generate cells_idx to make it map correctly to matrix
    ## also add cell_type column
    cells = pd.read_csv(cells_filename, sep='\t')
    cohort_cells = pd.read_csv(cohort_cells_filename, sep='\t')
    cohort_cells.index.name = 'cell_idx'
    cohort_cells = cohort_cells.reset_index(drop=False)
    cohort_cells = cohort_cells[['cell_idx', 'cell_id', 'cell_type']]
    cells = cells.merge(cohort_cells)

    # Florian's data has the metadata in there already, so need to remove
    cells = cells.drop(columns=['patient_id', 'tumor_supersite', 'tumor_subsite', 'sort_parameters', 'therapy'])

    ## Write
    cells.to_csv(cells_filename, sep='\t')




# download_analyses_data([{'pk': 1651, 'modified': '2020-05-20T17:46:52.921614-04:00', 'juno_storage': '/work/shah/isabl_data_lake/analyses/16/51/1651', 'patient_id': 'SPECTRUM-OV-031'}, {'pk': 1659, 'modified': '2020-05-20T17:46:15.225621-04:00', 'juno_storage': '/work/shah/isabl_data_lake/analyses/16/59/1659', 'patient_id': 'SPECTRUM-OV-002'}, {'pk': 1688, 'modified': '2020-05-21T00:31:30.208087-04:00', 'juno_storage': '/work/shah/isabl_data_lake/analyses/16/88/1688', 'patient_id': 'SPECTRUM-OV-007'}, {'pk': 1702, 'modified': '2020-05-20T22:11:20.254439-04:00', 'juno_storage': '/work/shah/isabl_data_lake/analyses/17/02/1702', 'patient_id': 'SPECTRUM-OV-003'}], '/data/mira')