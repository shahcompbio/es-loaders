import json


class scRNAParser():
    def __init__(self, filePath):
        self.path = filePath
        self.data = json.loads(open(filePath, 'r').read())

    def get_samples(self):
        keys = list(self.data.keys())
        samples = [key for key in keys if key.startswith('Sample')]
        return samples

    def get_cells(self, sample_id):
        sample = self.data[sample_id]
        return sample['celldata']['Barcode']

    def get_dim_red(self, sample_id):
        return self.data[sample_id]['tsne']

    def get_celltypes(self, sample_id):
        return self.data[sample_id]['cellassign']

    def get_gene_matrix(self, sample_id):
        return self.data[sample_id]['log_count_matrix']

    def get_rho(self):
        return self.data['rho']

    def get_statistics(self, sample_id):
        return self.data['statistics'][sample_id]

    def get_sites(self, sample_id):
        return self.data[sample_id]['site']
