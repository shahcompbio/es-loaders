import logging
import os
import pandas as pd

from mira.elasticsearch import load_genes
import mira.constants as constants

logger = logging.getLogger('mira_loading')

def load_gene_names(directory, host, port):
    logger.info("LOADING GENE LIST")

    filename = os.path.join(directory, constants.GENES_FILENAME)
    genes = pd.read_csv(filename, sep='\t')

    genes = list(genes['genes'])

    records = [{"gene": gene} for gene in genes]

    load_genes(records, host, port)
    return