import sys
import math
import yaml
from common.scrna_parser import scRNAParser
from utils.elasticsearch import load_records, load_record
from mira.mira_metadata_parser import single_sample
from rho_loader import get_rho_celltypes
from utils.cli import CliClient

SAMPLE_METADATA_INDEX = "sample_metadata"
SAMPLE_STATS_INDEX = "sample_stats"
SAMPLE_CELLS_INDEX = "sample_cells"
DASHBOARD_REDIM_INDEX = "dashboard_redim_"
DASHBOARD_GENES_INDEX = "dashboard_genes_"
DASHBOARD_ENTRY_INDEX = "dashboard_entry"


def load_analysis(filepath, dashboard_id, type, host="localhost", port=9200):
    print("====================== " + dashboard_id)
    print("Opening File")
    data = scRNAParser(filepath + ".rdata")

    if type is "sample":
        print("Load Sample Data")
        load_sample_metadata(dashboard_id, host=host, port=port)
        load_sample_cells(data, dashboard_id, host=host, port=port)
        load_sample_statistics(data, dashboard_id, host=host, port=port)

    load_dashboard_redim(data, type, dashboard_id, host=host, port=port)
    load_dashboard_genes(data, dashboard_id, host=host, port=port)

    load_dashboard_entry(type, dashboard_id, host=host, port=port)
    # Need rho loader (this only has to be done once)


def load_sample_metadata(sample_id, host="localhost", port=9200):
    print("LOADING SAMPLE METADATA: " + sample_id)
    metadata = single_sample(sample_id)

    sort_status = metadata["sort_parameters"].split(",")[2].strip()
    sort = {"U": "unsorted", "CD45+": "CD45P",
            "CD45-": "CD45N"}[sort_status]

    record = {
        "sample_id": sample_id,
        "patient_id": metadata["patient_id"],  # parse from sample_id
        "surgery": metadata["time"],  # parse S1/S2 from sample_id
        "treatment": metadata["therapy"],  # pre | post
        "site": metadata["tumour_site"],  # parse from sample_id
        "sort": sort  # parse from sample_id
    }
    print(" BEGINNING LOAD")
    print(record)
    load_record(SAMPLE_METADATA_INDEX, record, host=host, port=port)


def load_sample_statistics(data, sample_id, host="localhost", port=9200):

    print("LOADING SAMPLE STATS: " + sample_id)
    statistics = data.get_statistics()

    stats_records = get_stats_records_generator(statistics, sample_id)
    print(" BEGINNING LOAD")
    load_records(SAMPLE_STATS_INDEX, stats_records, host=host, port=port)


def get_stats_records_generator(stats, sample_id):
    for stat, value in stats.items():
        record = {
            "sample_id": sample_id,
            "stat": stat,
            "value": value
        }
        yield record


def load_sample_cells(data, sample_id, host="localhost", port=9200):
    print("LOADING SAMPLE CELLS: " + sample_id)
    cells = data.get_cells()
    celltypes = data.get_celltypes()

    rho_celltypes = get_rho_celltypes()
    celltype_probabilities = data.get_all_celltype_probability(rho_celltypes)

    cell_records = get_sample_cells_generator(
        cells, celltypes, rho_celltypes, celltype_probabilities, sample_id)
    print(" BEGINNING LOAD")
    load_records(SAMPLE_CELLS_INDEX, cell_records, host=host, port=port)


def get_sample_cells_generator(cells, celltypes, rho_celltypes, celltype_probabilities, sample_id):

    def get_cell_probabilities(cell):
        cell_probabilities = {}
        for celltype in rho_celltypes:
            cell_probabilities[celltype +
                               " probability"] = celltype_probabilities[celltype][cell]

        return cell_probabilities

    for cell in cells:
        cell_probabilities = get_cell_probabilities(cell)
        record = {
            "sample_id": sample_id,
            "cell_id": cell,
            "cell_type": celltypes[cell],
            **cell_probabilities
        }
        yield record


def load_dashboard_redim(data, type, dashboard_id, host="localhost", port=9200):
    print("LOADING DASHBOARD RE-DIM: " + dashboard_id)
    cells = data.get_cells()
    redim = data.get_re_dim(
        'scanorama_UMAP') if type is "patient" else data.get_re_dim()

    redim_records = get_redim_record_generator(cells, redim, dashboard_id)
    print(" BEGINNING LOAD")
    load_records(DASHBOARD_REDIM_INDEX + dashboard_id.lower(),
                 redim_records, host=host, port=port)


def get_redim_record_generator(cells, redim, dashboard_id):
    for cell in cells:
        record = {
            "cell_id": cell,
            "x": redim[cell][0],
            "y": redim[cell][1],
            "dashboard_id": dashboard_id
        }
        yield record


def load_dashboard_genes(data, dashboard_id, host="localhost", port=9200):
    print("LOADING DASHBOARD GENES: " + dashboard_id)
    cells = data.get_cells()
    genes = data.get_gene_matrix()

    gene_records = get_gene_record_generator(cells, genes, dashboard_id)
    print(" BEGINNING LOAD")
    load_records(DASHBOARD_GENES_INDEX + dashboard_id.lower(),
                 gene_records, host=host, port=port)


def get_gene_record_generator(cells, gene_matrix, dashboard_id):
    for cell in cells:
        genes = gene_matrix[cell]

        for gene, log_count in genes.items():
            record = {
                "cell_id": cell,
                "gene": gene,
                "log_count": log_count,
                "dashboard_id": dashboard_id
            }
            yield record


def load_dashboard_entry(type, dashboard_id, host="localhost", port=9200):
    print("LOADING DASHBOARD ENTRY: " + dashboard_id)
    record = {
        "type": type,
        "dashboard_id": dashboard_id
    }
    print(" BEGINNING LOAD")
    print(record)
    load_record(DASHBOARD_ENTRY_INDEX, record, host=host, port=port)


def main():
    CLI = CliClient('Mira Loader')
    CLI.add_filepath_argument(isFilepath=False)
    CLI.add_elasticsearch_arguments()

    mira_group = CLI.parser.add_argument_group("Mira")
    mira_group.add_argument('-id', dest="mira_id", required=True)
    mira_group.add_argument('-type', dest="mira_type", default="sample")
    print("STARTING MIRA LOAD")
    args = CLI.get_args()
    print("STARTING LOAD")
    print(args)
    load_analysis(args.file_root, args.mira_id, args.mira_type,
                  host=args.es_host, port=args.es_port)


if __name__ == '__main__':
    main()
