import numpy
import collections
import scipy
import scipy.spatial as spatial

from common.singlecellexperiment import SingleCellExperiment


class scRNAParser():
    def __init__(self, filePath):
        self.path = filePath
        self.data = SingleCellExperiment.fromRData(self.path)

    def get_cells(self):
        samples = self.data.colData["Sample"]
        samples = [sample.split("/")[-1]
                   for sample in self.data.colData["Sample"] if "_IGO_" in sample.split("/")[-1]]

        if len(samples) == 0:
            samples = [sample.split("/")[-2]
                       for sample in self.data.colData["Sample"]]
        barcodes = self.data.colData["Barcode"]
        sample_barcodes = [name[0] + ":" + name[1]
                           for name in zip(barcodes, samples)]
        return sample_barcodes

    def get_sample_list(self):
        samples = [sample.split("/")[-1]
                   for sample in self.data.colData["Sample"] if "_IGO_" in sample.split("/")[-1]]

        if len(samples) == 0:
            samples = [sample.split("/")[-2]
                       for sample in self.data.colData["Sample"]]

        samples = set(samples)

        return samples

    def get_samples(self):
        barcodes = self.get_cells()
        samples = self.data.colData["Sample"]
        samples = [sample.split("/")[-1]
                   for sample in self.data.colData["Sample"] if "_IGO_" in sample.split("/")[-1]]

        if len(samples) == 0:
            samples = [sample.split("/")[-2]
                       for sample in self.data.colData["Sample"]]

        return dict(zip(barcodes, samples))

    def get_dim_red(self, embedding="UMAP", min_neighbors=5, neighbor_dist=1):
        barcodes = self.get_cells()
        _embedding = self.data.getReducedDims(embedding)
        embedding = zip(barcodes, _embedding)

        point_tree = spatial.cKDTree(_embedding)
        filtered_embedding = dict()
        for barcode, point in embedding:
            neighbors = point_tree.query_ball_point(point, neighbor_dist)
            if len(neighbors) > min_neighbors:
                filtered_embedding[barcode] = point

        return dict(filtered_embedding)

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
        barcodes = self.get_cells()
        genes = self.data.rownames
        matrix = self.data.assays[assay]

        return [genes, barcodes, matrix]

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
            list(filter(lambda x: x < 20, coldata["pct_counts_mitochondrial"])))
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
        barcodes = self.get_cells()
        coldata = self.data.colData
        celltype = scRNAParser.unformat_celltype(celltype)
        probabilities = [0.0 for _ in barcodes]
        if celltype in coldata:
            probabilities = coldata[celltype]
        return dict(zip(barcodes, probabilities))

    def get_pathway(self, pathway):
        coldata = self.data.colData
        barcodes = self.get_cells()
        assert pathway in coldata, "DNA repair type not computed."
        return dict(zip(barcodes, coldata[pathway]))

    def get_exhausted_probability(self):
        # Needs to grab from a different rData file, but this is the gist of the code
        coldata = self.data.colData
        barcodes = self.get_cells()
        assert 'Exhausted_prob' in coldata, "Exhausted probability not computed."
        return dict(zip(barcodes, coldata['Exhausted_prob']))


if __name__ == '__main__':
    parser = scRNAParser("SPECTRUM-OV-041_S1_CD45N_INFRACOLIC_OMENTUM.rdata")
    sample_id = parser.get_samples()

    # site = parser.data.colData["site"]
    # print(parser.get_dim_red())
    # print(parser.get_gene_matrix("SPECTRUM-OV-041_S1_CD45N_INFRACOLIC_OMENTUM"))
    # site = parser.data.colData["cell_type"]
    # print(site)
    # print(site)

    # print(parser.get_cells(sample_id))
    # print(parser.get_dim_red(sample_id))
    # print(parser.get_celltypes(sample_id))
    # print(parser.get_assays(sample_id))
    # print(parser.get_gene_matrix(sample_id))
    # print(parser.get_statistics(sample_id))
    # print(parser.get_celltype_probability("Monocyte/Macrophage"))
    # print(parser.get_pathway("repairtype"))
