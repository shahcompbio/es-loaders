import numpy
import collections

from common.singlecellexperiment import SingleCellExperiment


class scRNAParser():
    def __init__(self, filePath):
        self.path = filePath
        self.data = SingleCellExperiment.fromRData(self.path)

    def get_samples(self):
        return set(self.data.colData["sample"])

    def get_cells(self):
        samples = self.data.colData["sample"]
        barcodes = self.data.colData["Barcode"]
        sample_barcodes = zip(barcodes, samples)
        sample_barcodes = filter(
            lambda cell: cell[0] in barcodes, sample_barcodes)
        return dict(sample_barcodes)

    def get_re_dim(self, embedding="UMAP"):
        barcodes = self.get_cells()
        embedding = zip(barcodes, self.data.getReducedDims(embedding))
        sample_embedding = filter(lambda cell: cell[0] in barcodes, embedding)
        return dict(sample_embedding)

    @staticmethod
    def format_celltype(cell_type):
        cell_type = cell_type.replace(".", " ")
        if "Monocyte" in cell_type:
            cell_type = cell_type.replace(" ", "/")
        return cell_type

    @staticmethod
    def unformat_celltype(cell_type):
        cell_type = cell_type.replace(" ", ".")
        if "Monocyte" in cell_type:
            cell_type = cell_type.replace("/", ".")
        return cell_type

    def get_celltypes(self):
        barcodes = self.get_cells()
        celltypes = map(scRNAParser.format_celltype,
                        self.data.colData["cell_type"])
        barcoded_celltypes = zip(barcodes, celltypes)
        sample_celltypes = filter(
            lambda cell: cell[0] in barcodes, barcoded_celltypes)
        return dict(sample_celltypes)

    def get_assays(self):
        return self.data.assayNames

    def get_gene_matrix(self, assay="logcounts"):
        coldata = self.data.colData
        rowdata = self.data.rowData
        matrix = self.data.assays[assay].tolist()
        assay_matrix = collections.defaultdict(dict)
        for symbol, row in zip(rowdata["Symbol"], matrix):
            for barcode, cell in zip(coldata["Barcode"], row):
                if float(cell) != 0.0:
                    assay_matrix[barcode][symbol] = cell
        return dict(assay_matrix)

    def get_statistics(self):
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

        # # Keeping for consistency, no way to pull from SCE object currently
        # statistics["Chemistry"] = "Single Cell 3' v3"
        # statistics["Mean Reads per Cell"] = "NA"
        # statistics["Sequencing Saturation"] = "NA"
        # statistics["Valid Barcodes"] = "NA"
        # ##################################################################

        statistics["Mito20"] = len(
            list(filter(lambda x: x < 20, coldata["pct_counts_mito"])))
        statistics["Estimated Number of Cells"] = len(coldata["Barcode"])
        statistics["Median UMI Counts"] = int(numpy.median(total_counts))
        statistics["Number of Reads"] = int(numpy.sum(cell_counts))
        statistics["Median Genes per Cell"] = int(median_genes_per_cell)
        statistics["Number of Genes"] = genes_with_expression
        return statistics

    def get_all_celltype_probability(self, celltypes):
        probabilities = [self.get_celltype_probability(
            celltype) for celltype in celltypes]

        return dict(zip(celltypes, probabilities))

    def get_celltype_probability(self, celltype):
        coldata = self.data.colData
        celltype = scRNAParser.unformat_celltype(celltype)
        probabilities = [0.0 for _ in coldata["Barcode"]]
        if celltype in coldata:
            probabilities = coldata[celltype]
        return dict(zip(coldata["Barcode"], probabilities))

    def get_pathway(self, pathway):
        coldata = self.data.colData
        assert pathway in coldata, "DNA repair type not computed."
        return dict(zip(coldata["Barcode"], coldata[pathway]))


if __name__ == '__main__':
    parser = scRNAParser("SPECTRUM-OV-002_S1_CD45N_RIGHT_ADNEXA.rdata")
    sample_id = parser.get_samples()

    site = parser.data.colData["site"]
    # print(site)

    # print(parser.get_cells(sample_id))
    # print(parser.get_dim_red(sample_id))
    # print(parser.get_celltypes(sample_id))
    # print(parser.get_assays(sample_id))
    print(parser.get_gene_matrix(sample_id))
    # print(parser.get_statistics(sample_id))
    # print(parser.get_celltype_probability("Monocyte/Macrophage"))
    # print(parser.get_pathway("repairtype"))
