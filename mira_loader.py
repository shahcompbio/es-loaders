import sys
import math
import yaml
from common.scrna_parser import scRNAParser
from utils.elasticsearch import ElasticsearchClient

from utils.cli import CliClient


class dimRedLoader():

    YAML_NAME = "patient_metadata.yaml"

    METADATA_INDEX_NAME = "patient_metadata"
    CELL_INDEX_NAME = "_cells"
    GENE_INDEX_NAME = "_genes"
    RHO_INDEX_NAME = "_rho"

    # TODO: host + port variables
    def __init__(self):
        pass

    def load_data(self, dir_path, host, port):
        print("PARSING YAML FILE")
        yaml_data = self._read_yaml(dir_path + self.YAML_NAME)

        patient_id = yaml_data["patient_id"]
        sample_ids = yaml_data["sample_ids"]

        print("LOADING DATA: " + patient_id)

        for sample_id in sample_ids:
            self._load_sample_data(patient_id, sample_id, dir_path, host, port)

    def _read_yaml(self, yaml_path):
        with open(yaml_path, 'r') as stream:
            yaml_data = yaml.safe_load(stream)

        return yaml_data

    def _load_sample_data(self, patient_id, sample_id, dir_path, host, port):
        print("READING " + sample_id)
        data_obj = self._read_file_(dir_path + sample_id + ".json")

        print("TRANSFORM + LOADING " + sample_id)
        self._transform_and_load_data(
            data_obj, patient_id, sample_id, host, port)

    def _read_file_(self, file):
        data = scRNAParser(file)
        return data

    def _transform_and_load_data(self, data, patient_id, sample_id, host, port):
        es = ElasticsearchClient(host=host, port=port)

        statistics = data.get_statistics(sample_id)

        print("LOADING PATIENT-SAMPLE RECORD")
        es.load_record(self.METADATA_INDEX_NAME, self._get_patient_sample_record(
            patient_id, sample_id, statistics))

        cells = data.get_cells(sample_id)
        dim_red = data.get_dim_red(sample_id)
        celltypes = data.get_celltypes(sample_id)

        filtered_cells = list(filter(lambda cell: cell in celltypes, cells))

        self._transform_and_load_cells(
            patient_id, sample_id, filtered_cells, dim_red, celltypes, es)

        genes = data.get_gene_matrix(sample_id)
        self._transform_and_load_genes(
            patient_id, sample_id, filtered_cells, genes, es)

        rho = data.get_rho()
        self._transform_and_load_cellassign_rho(patient_id, sample_id, rho, es)

    #############################################
    #
    #                   QC
    #
    #############################################

    def _get_patient_sample_record(self, patient_id, sample_id, statistics):
        return {
            "patient_id": patient_id,
            "sample_id": sample_id,
            "mito5": int(statistics["Mito5"]),
            "mito10": int(statistics["Mito10"]),
            "mito15": int(statistics["Mito15"]),
            "mito20": int(statistics["Mito20"]),
            "num_cells": int(statistics["Estimated Number of Cells"]),
            "num_reads": int(statistics["Number of Reads"]),
            "num_genes": int(statistics["Total Genes Detected"]),
            "percent_barcodes": statistics["Valid Barcodes"],
            "sequencing_sat": statistics["Sequencing Saturation"],
            "median_umi": int(statistics["Median UMI Counts per Cell"])
        }

    #############################################
    #
    #                   CELLS
    #
    #############################################

    def _transform_and_load_cells(self,
                                  patient_id, sample_id, cells, dim_red,  celltypes, es):

        cell_records = self._cell_record_generator(
            patient_id, sample_id, cells, dim_red, celltypes)

        print("Loading Cells: " + sample_id)

        es.load_in_bulk(patient_id.lower() +
                        self.CELL_INDEX_NAME, cell_records)

    def _cell_record_generator(self, patient_id, sample_id, cells, dim_red,  celltypes):
        for cell in cells:
            record = {
                "cell_id": cell,
                "patient_id": patient_id,
                "sample_id": sample_id,
                "x": dim_red[cell][0],
                "y": dim_red[cell][1],
                "cell_type": celltypes[cell]
            }
            yield record

    #############################################
    #
    #                   GENES
    #
    #############################################

    def _transform_and_load_genes(self, patient_id, sample_id, cells, genes, es):
        gene_records = self._gene_record_generator(
            patient_id, sample_id, cells, genes)

        print("Loading Genes: " + sample_id)

        es.load_in_bulk(patient_id.lower() +
                        self.GENE_INDEX_NAME, gene_records)

    def _gene_record_generator(self, patient_id, sample_id, cells, gene_matrix):
        for cell in cells:
            genes = gene_matrix[cell]

            for gene, count in genes.items():
                record = {
                    "cell_id": cell,
                    "patient_id": patient_id,
                    "sample_id": sample_id,
                    "gene": gene,
                    "count": count
                }
                yield record

    #############################################
    #
    #                   RHO
    #
    #############################################

    def _transform_and_load_cellassign_rho(self, patient_id, sample_id, rho, es):

        rho_records = self._rho_record_generator(patient_id, sample_id, rho)

        print("Loading Rho: " + sample_id)

        es.load_in_bulk(patient_id.lower() +
                        self.RHO_INDEX_NAME, rho_records)

    def _rho_record_generator(self, patient_id, sample_id, rho):
        for celltype, marker_genes in rho.items():
            for gene in marker_genes:

                record = {
                    "patient_id": patient_id,
                    "sample_id": sample_id,
                    "celltype": celltype,
                    "marker_gene": gene
                }

                yield record


def main():
    CLI = CliClient('Mira Loader')
    CLI.add_filepath_argument(isFilepath=False)
    CLI.add_elasticsearch_arguments()

    print("STARTING ALHENA LOAD")
    args = CLI.get_args()
    print("STARTING LOAD")
    loader = dimRedLoader()
    loader.load_data(args.file_root,
                     host=args.es_host, port=args.es_port)


if __name__ == '__main__':
    main()
