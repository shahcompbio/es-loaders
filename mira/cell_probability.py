import json


class CellTypeProbability(object):
    def __init__(self, filepath, sample_list):
        data = {}
        for sample in sample_list:
            filename = filepath + sample + "_meta.json"
            with open(filename) as json_file:
                data_file = json.load(json_file)
                data[sample] = data_file

        self.data = data

    def get_cell_probabilities(self, sample, barcode, celltypes=None):

        if celltypes is None:
            celltypes = [format_celltype(celltype)
                         for celltype in self.data[sample].keys()]

        probabilities = {}
        sheet = self.data[sample]

        for celltype in celltypes:
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
