import json
from common.scrna_parser import scRNAParser


class CellTypeProbability(object):
    def __init__(self, filepath, sample_list, celltypes):
        data = {}
        self.celltypes = celltypes

        for sample in sample_list:
            try:
                filename = filepath + sample + "_meta.json"
                with open(filename) as json_file:
                    data_file = json.load(json_file)
                    data[sample] = data_file
            except FileNotFoundError as err:
                print(err)
                filename = filepath + sample + ".rdata"
                data_file = scRNAParser(filename)
                data[sample] = data_file.get_all_celltype_probability(
                    celltypes)

        self.data = data

    def get_cell_probabilities(self, sample, barcode):

        probabilities = {}
        sheet = self.data[sample]

        for celltype in self.celltypes:
            celltype_data = sheet[unformat_celltype(
                celltype)] if unformat_celltype(celltype) in sheet else {}

            probabilities[celltype +
                          " probability"] = celltype_data[barcode] if barcode in celltype_data else 0.0

        return probabilities


def format_celltype(cell_type):
    cell_type = cell_type.replace(".", " ")
    if "Monocyte" in cell_type:
        cell_type = cell_type.replace(" ", "/")
    return cell_type


def unformat_celltype(cell_type):
    cell_type = cell_type.replace(" ", ".")
    if "Monocyte" in cell_type:
        cell_type = cell_type.replace("/", ".")
    return cell_type

# print(yo.data['SPECTRUM-OV-002_S1_CD45N_RIGHT_ADNEXA'].keys())
