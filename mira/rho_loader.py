
import os
import pandas as pd
import numpy as np
import requests
import io
import mira.constants as constants
from mira.elasticsearch import get_cell_type_count

import logging
logger = logging.getLogger("mira_loading")


def generate_dashboard_rho(cell_types, dashboard_id, host, port):
    ## Assumes that cell data is already loaded
    records = []

    for cell_type_record in cell_types:
        count = get_cell_type_count(cell_type_record["cell_type"], dashboard_id, host, port)

        records.append({
            **cell_type_record,
            "dashboard_id": dashboard_id,
            "count": count
        })        


    return records


def download_rho_data():
    logger.info("======================= Downloading marker genes")

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

    return data