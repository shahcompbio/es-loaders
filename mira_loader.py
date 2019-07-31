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
        # TODO: If data exists, load patient data
        self._load_dashboard_data(yaml_data["patient_data"],
                                  patient_id=patient_id, dir_path=dir_path, host=host, port=port)

        # TODO: If sample data exists, load
        for sample_id_obj in sample_ids:
            sample_id = "CD45" + \
                sample_id_obj["CD45"] + "_" + sample_id_obj["site"]
            print(sample_id)
            self._load_dashboard_data(sample_id_obj["data"],
                                      patient_id=patient_id, sample_id=sample_id, dir_path=dir_path, host=host, port=port)

    def _read_yaml(self, yaml_path):
        with open(yaml_path, 'r') as stream:
            yaml_data = yaml.safe_load(stream)

        return yaml_data

    def _load_dashboard_data(self, data_filename, patient_id, dir_path, host, port, sample_id=None):

        id_name = patient_id if sample_id is None else sample_id

        print("READING " + id_name)
        data_obj = self._read_file_(dir_path + data_filename + ".json")

        print("TRANSFORM + LOADING " + id_name)
        self._transform_and_load_data(
            data_obj, data_filename, patient_id=patient_id, sample_id=sample_id, host=host, port=port)

    def _read_file_(self, file):
        data = scRNAParser(file)
        return data

    def _transform_and_load_data(self, data, data_name, patient_id, sample_id, host, port):
        es = ElasticsearchClient(host=host, port=port)

        print("LOADING PATIENT-SAMPLE RECORD")
        es.load_record(self.METADATA_INDEX_NAME, self._get_patient_sample_record(
            patient_id, sample_id, data, data_name))

        dashboard_id = patient_id if sample_id is None else sample_id
        ids = {"patient_id": patient_id} if sample_id is None else {
            "patient_id": patient_id, "sample_id": sample_id}

        cells = data.get_cells(data_name)
        dim_red = data.get_dim_red(data_name)
        celltypes = data.get_celltypes(data_name)

        sites = data.get_sites(data_name) if sample_id is None else {}

        filtered_cells = list(filter(lambda cell: cell in celltypes, cells))

        self._transform_and_load_cells(
            ids, dashboard_id, filtered_cells, dim_red, celltypes, sites, es)

        genes = data.get_gene_matrix(data_name)
        self._transform_and_load_genes(
            ids, dashboard_id, filtered_cells, genes, es)

        rho = data.get_rho()
        self._transform_and_load_cellassign_rho(ids, dashboard_id, rho, es)

    #############################################
    #
    #                   QC
    #
    #############################################

    def _get_patient_sample_record(self, patient_id, sample_id, data, data_name):

        if sample_id is None:
            return {
                "patient_id": patient_id
            }
        else:
            statistics = data.get_statistics(data_name)
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
                "mean_reads": int(statistics["Mean Reads per Cell"]),
                "median_genes": int(statistics["Median Genes per Cell"]),
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
                                  ids, dashboard_id, cells, dim_red,  celltypes, sites, es):

        cell_records = self._cell_record_generator(
            ids, cells, dim_red, celltypes, sites)

        print("Loading Cells: " + dashboard_id)

        es.load_in_bulk(ids["patient_id"].lower() +
                        self.CELL_INDEX_NAME, cell_records)

    def _cell_record_generator(self, ids, cells, dim_red,  celltypes, sites):

        for cell in cells:
            site = {} if "sample_id" in ids else {
                "site": sites[cell] if sites['cell'] != 'INFERIOR_OMENTUM' else 'INFRACOLIC_OMENTUM'}
            record = {
                "cell_id": cell,
                "x": dim_red[cell][0],
                "y": dim_red[cell][1],
                "cell_type": celltypes[cell],
                **site,
                **ids
            }
            yield record

    #############################################
    #
    #                   GENES
    #
    #############################################

    def _transform_and_load_genes(self, ids, dashboard_id, cells, genes, es):
        gene_records = self._gene_record_generator(
            ids, cells, genes)

        print("Loading Genes: " + dashboard_id)

        es.load_in_bulk(ids["patient_id"].lower() +
                        self.GENE_INDEX_NAME, gene_records)

    def _gene_record_generator(self, ids, cells, gene_matrix):
        for cell in cells:
            genes = gene_matrix[cell]

            for gene, count in genes.items():
                record = {
                    "cell_id": cell,
                    "gene": gene,
                    "count": count,
                    **ids
                }
                yield record

    #############################################
    #
    #                   RHO
    #
    #############################################

    def _transform_and_load_cellassign_rho(self, ids,
                                           dashboard_id, rho, es):

        rho_records = self._rho_record_generator(ids, rho)

        print("Loading Rho: " + dashboard_id)

        es.load_in_bulk(ids["patient_id"].lower() +
                        self.RHO_INDEX_NAME, rho_records)

    def _rho_record_generator(self, ids, rho):
        for celltype, marker_genes in rho.items():
            for gene in marker_genes:

                record = {
                    "celltype": celltype,
                    "marker_gene": gene,
                    **ids
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
