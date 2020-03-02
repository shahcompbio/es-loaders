
import click
import sys
import math
import yaml
import logging
import itertools
import numpy
from mira.metadata_parser import MiraMetadata
from mira.rho_loader import get_rho_celltypes
from mira.mira_cleaner import clean_analysis
from common.scrna_parser import scRNAParser
from mira.cell_probability import CellTypeProbability
from utils.elasticsearch import load_records, load_record


# SAMPLE_METADATA_INDEX = "sample_metadata"
SAMPLE_STATS_INDEX = "sample_stats"
# SAMPLE_CELLS_INDEX = "sample_cells"
# DASHBOARD_REDIM_INDEX = "dashboard_redim_"

DASHBOARD_CELLS_INDEX = "dashboard_cells"
DASHBOARD_GENES_INDEX = "dashboard_genes_"
DASHBOARD_ENTRY_INDEX = "dashboard_entry"
logger = logging.getLogger('mira_loading')


def load_analysis(filepath, dashboard_id, type, host, port, metadata=None):

    if metadata is None:
        metadata = MiraMetadata()

    logger.info("====================== " + dashboard_id)
    file = _get_filepath(filepath, dashboard_id, type)
    logger.info("Opening File: " + file)
    data = scRNAParser(file)
    if type == "sample":
        logger.info("Load Sample Data")
        load_sample_statistics(data, dashboard_id, host=host, port=port)

    load_dashboard_cells(data, type, dashboard_id,
                         metadata, filepath, host=host, port=port)
    load_dashboard_genes(data, type, dashboard_id, host=host, port=port)
    load_dashboard_entry(type, dashboard_id,
                         metadata=metadata, host=host, port=port)


def _get_filepath(filepath, dashboard_id, type):
    if filepath.endswith(".rdata"):
        return filepath
    else:
        return filepath + dashboard_id + ".rdata"


def load_sample_statistics(data, sample_id, host="localhost", port=9200):

    logger.info("LOADING SAMPLE STATS: " + sample_id)
    statistics = data.get_statistics()

    stats_records = get_stats_records_generator(statistics, sample_id)
    logger.info(" BEGINNING LOAD")
    load_records(SAMPLE_STATS_INDEX, stats_records, host=host, port=port)


def get_stats_records_generator(stats, sample_id):
    for stat, value in stats.items():
        record = {
            "sample_id": sample_id,
            "stat": stat,
            "value": value
        }
        yield record


def load_dashboard_cells(data, type, dashboard_id, metadata, filepath, host="localhost", port=9200):
    logger.info("LOADING DASHBOARD CELLS: " + dashboard_id)

    redim = data.get_dim_red(
        'scanorama_UMAP') if type == "patient" else data.get_dim_red()

    cells = list(redim.keys())

    samples = data.get_samples()
    celltypes = data.get_celltypes()
    rho_celltypes = get_rho_celltypes()

    sample_list = [metadata.get_igo_to_sample_id(
        sample) for sample in data.get_sample_list()]
    celltype_probabilities = CellTypeProbability(
        filepath, sample_list, rho_celltypes)

    cell_records = get_cells_generator(
        cells, samples, celltypes, rho_celltypes, celltype_probabilities, redim, dashboard_id, metadata)

    logger.info(" BEGINNING LOAD")
    load_records(DASHBOARD_CELLS_INDEX, cell_records, host=host, port=port)


def get_cells_generator(cells, samples, celltypes, rho_celltypes, celltype_probabilities, redim, dashboard_id, metadata):

    for cell in cells:
        [barcode, sample] = cell.split(":")
        sample = metadata.get_igo_to_sample_id(sample)
        cell_probabilities = celltype_probabilities.get_cell_probabilities(
            sample, barcode)

        record = {
            "dashboard_id": dashboard_id,
            "cell_id": cell,
            "cell_type": celltypes[cell],
            "x": redim[cell][0],
            "y": redim[cell][1],
            "sample_id": metadata.get_igo_to_sample_id(samples[cell]),
            ** cell_probabilities
        }
        yield record


def load_dashboard_genes(data, type, dashboard_id, host="localhost", port=9200):
    logger.info("LOADING DASHBOARD GENES: " + dashboard_id)

    redim = data.get_dim_red(
        'scanorama_UMAP') if type == "patient" else data.get_dim_red()

    [gene_symbols, cell_barcodes, gene_matrix] = data.get_gene_matrix()

    gene_records = get_gene_record_generator(
        redim, gene_symbols, cell_barcodes, gene_matrix, dashboard_id)
    logger.info(" BEGINNING LOAD")
    load_records(DASHBOARD_GENES_INDEX + dashboard_id.lower(),
                 gene_records, host=host, port=port)


def get_gene_record_generator(redim, gene_symbols, cell_barcodes, gene_matrix, dashboard_id):
    cells = list(redim.keys())
    num_genes, num_cells = gene_matrix.shape
    if isinstance(gene_matrix, numpy.ndarray):
        for row in range(num_genes):
            for column in range(num_cells):
                data = gene_matrix[i, j]
                if float(data) != 0.0:
                    record = {
                        "cell_id": cell_barcodes[column],
                        "gene": gene_symbols[row],
                        "log_count": float(data),
                        "dashboard_id": dashboard_id,
                        "x": redim[cell_barcodes[column]][0],
                        "y": redim[cell_barcodes[column]][1]
                    }
                    yield record

    else:
        for row in range(num_genes):
            k = gene_matrix.indptr[row]
            l = gene_matrix.indptr[row+1]
            for data, column in zip(gene_matrix.data[k:l], gene_matrix.indices[k:l]):
                if cell_barcodes[column] in cells and float(data) != 0.0:
                    record = {
                        "cell_id": cell_barcodes[column],
                        "gene": gene_symbols[row],
                        "log_count": float(data),
                        "dashboard_id": dashboard_id,
                        "x": redim[cell_barcodes[column]][0],
                        "y": redim[cell_barcodes[column]][1]
                    }
                    yield record


def load_dashboard_entry(type, dashboard_id, metadata, host="localhost", port=9200):

    logger.info("LOADING DASHBOARD ENTRY: " + dashboard_id)

    metadata_records = _get_metadata(type, dashboard_id, metadata)

    record = {
        "dashboard_id": dashboard_id,
        "type": type,
        "patient_id": metadata_records[0]["patient_id"],
        "sort": list(set([datum['sort_parameters'] for datum in metadata_records])),
        "sample_ids": [datum["nick_unique_id"] for datum in metadata_records],
        "surgery": list(set([datum["time"] for datum in metadata_records])),
        "treatment": list(set([datum["therapy"] for datum in metadata_records])),
        "site": list(set([datum["tumour_site"] for datum in metadata_records]))
    }
    logger.info(" BEGINNING LOAD")
    logger.info(record)
    load_record(DASHBOARD_ENTRY_INDEX, record, host=host, port=port)


def _get_metadata(type, dashboard_id, metadata):
    if type == "sample":
        return metadata.get_data([dashboard_id])
    else:  # assume patient
        sample_ids = metadata.support_sample_ids(dashboard_id)
        return metadata.get_data(sample_ids)


if __name__ == '__main__':
    pass
