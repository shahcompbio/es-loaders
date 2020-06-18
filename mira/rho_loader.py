
import os
import pandas as pd
import numpy as np
import mira.constants as constants

import logging
logger = logging.getLogger("mira_loading")


def download_rho_data(token):
    logger.info("======================= Downloading marker genes")
    marker_genes = pd.read_csv(constants.MARKER_GENES_URL+ "?token=" + token)
    marker_genes = marker_genes.transpose()

    gene_list = marker_genes[:'Unnamed: 0'].values.tolist()[0]
    marker_genes.columns = gene_list
    marker_genes = marker_genes.iloc[1:]

    marker_genes.index.name = 'cell_type'
    marker_genes = marker_genes.reset_index(drop=False)
    marker_genes['cell_type'] = marker_genes['cell_type'].str.replace('.', ' ')
    cell_types = marker_genes['cell_type'].values

    rows, cols = np.where(marker_genes.values == 1)
    marker_gene_list = list(zip(marker_genes.index[rows], marker_genes.columns[cols]))

    
    logger.info(f'{len(cell_types)} cell types with total {len(marker_gene_list)} marker genes')

    data = [{"cell_type": cell_types[marker_pair[0]], "gene": marker_pair[1]} for marker_pair in marker_gene_list]

    return data