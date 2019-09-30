import json
import numpy
import os

from singlecellexperiment import SingleCellExperiment
from genemarkermatrix import GeneMarkerMatrix

class scRNAParser():
    def __init__(self, filePath):
        self.path = filePath
        self.data = SingleCellExperiment.fromRData(self.path)

    def get_samples(self):
        return set(self.data.colData["sample"])
        
    def get_cells(self, sample_id):
        samples = self.data.colData["sample"]
        barcodes = self.data.colData["Barcode"]
        sample_barcodes = zip(barcodes,samples)
        sample_barcodes = filter(lambda cell: cell[0] in barcodes, sample_barcodes)
        return dict(sample_barcodes)

    def get_dim_red(self, sample_id, embedding="UMAP"):
        barcodes = self.get_cells(sample_id=sample_id)
        embedding = zip(barcodes,self.data.getReducedDims(embedding))
        sample_embedding = filter(lambda cell: cell[0] in barcodes, embedding)
        return dict(sample_embedding)

    @staticmethod
    def format_celltype(cell_type):
        cell_type = cell_type.replace("."," ")
        if "Monocyte" in cell_type: cell_type = cell_type.replace(" ","/")
        return cell_type

    @staticmethod
    def unformat_celltype(cell_type):
        cell_type = cell_type.replace(" ",".")
        if "Monocyte" in cell_type: cell_type = cell_type.replace("/",".")
        return cell_type

    def get_celltypes(self, sample_id):
        barcodes = self.get_cells(sample_id=sample_id)
        celltypes = map(scRNAParser.format_celltype, self.data.colData["cell_type"])
        barcoded_celltypes = zip(barcodes,celltypes)
        sample_celltypes = filter(lambda cell: cell[0] in barcodes, barcoded_celltypes)
        return dict(sample_celltypes)

    def get_assays(self, sample_id):
        return self.data.assayNames

    def get_gene_matrix(self, sample_id, assay="logcounts"):
        return self.data.get_assay(assay)

    @staticmethod
    def get_rho(filename=None):
        if not filename:
            package_directory = os.path.dirname(os.path.abspath(__file__))
            filename = os.path.join(package_directory,"markers.yaml")
        assert os.path.exists(filename), "Rho yaml not found."
        matrix = GeneMarkerMatrix.read_yaml(filename)
        return matrix.to_json()

    def get_statistics(self, sample_id):
        count_assay = self.data.get_assay("counts")
        coldata = self.data.colData
        rowdata = self.data.rowData

        total_counts = coldata["total_counts"]
        cell_counts = numpy.sum(count_assay)
        gene_counts = numpy.sum(count_assay, axis=1)

        genes_with_expression = len(list(filter(lambda x: x > 0, gene_counts)))
        genes_per_cell = numpy.count_nonzero(count_assay, axis=0)
        median_genes_per_cell = numpy.median(genes_per_cell)

        statistics = dict()
        statistics["Sample"] = sample_id

        #Keeping for consistency, no way to pull from SCE object currently
        statistics["Chemistry"] = "Single Cell 3' v3"
        statistics["Mean Reads per Cell"] = "NA"
        statistics["Sequencing Saturation"] = "NA"
        statistics["Valid Barcodes"] = "NA"
        ##################################################################

        statistics["Mito20"] = len(list(filter(lambda x: x < 20, coldata["pct_counts_mito"])))
        statistics["Estimated Number of Cells"] = len(coldata["Barcode"])
        statistics["Median UMI Counts"] = str(int(numpy.median(total_counts)))
        statistics["Number of Reads"] = str(int(numpy.sum(cell_counts)))
        statistics["Median Genes per Cell"] = str(int(median_genes_per_cell))
        statistics["Number of Genes"] = str(genes_with_expression)
        return statistics

    def get_celltype_probability(self, celltype):
        coldata = self.data.colData
        celltype = scRNAParser.unformat_celltype(celltype)
        assert celltype in coldata, 'Cell type not found - {}'.format(celltype)
        return dict(zip(coldata["Barcode"],coldata[celltype]))

    def get_pathway(self, pathway):
        coldata = self.data.colData
        assert pathway in coldata, "DNA repair type not computed."
        return dict(zip(coldata["Barcode"],coldata[pathway]))


if __name__ == '__main__':
    parser = scRNAParser("SPECTRUM-OV-002_S1_CD45N_RIGHT_ADNEXA.rdata")
    sample_id = parser.get_samples()

    site = parser.data.colData["site"]
    print(site)

    # print(parser.get_cells(sample_id))
    # print(parser.get_dim_red(sample_id))
    # print(parser.get_celltypes(sample_id))
    # print(parser.get_assays(sample_id))
    # print(parser.get_gene_matrix(sample_id))
    # print(parser.get_statistics(sample_id))
    # print(parser.get_celltype_probability("Monocyte/Macrophage"))
    # print(parser.get_pathway("repairtype"))




