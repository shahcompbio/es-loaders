import os
import pandas as pd
from utils.elasticsearch import load_records, load_record


## SCHEMA
#
# { 
#     cell_id: <>
#     cell_type: <>
#     x: <>
#     y: <>
#     sample: <>
#     sort: <>
#     genes: [{
#     	gene: <>
#         logcount: <>
#     },{
#     	gene: <>
#         logcount: <>
#     },...]
# }


def load_data(directory, patient_id):
    cells_filename = os.path.join(directory, 'cells.tsv')
    genes_filename = os.path.join(directory, 'genes.tsv')
    matrix_filename = os.path.join(directory, 'matrix.mtx')

    cells = pd.read_csv(cells_filename, sep='\t')
    cells.index.name = 'cell_id'
    cells = cells.reset_index(drop=False)
    cells.index.name = 'cell_idx'
    cells = cells.reset_index(drop=False)

    genes = pd.read_csv(genes_filename, sep='\t')
    genes.index.name = 'gene_idx'
    genes = genes.reset_index(drop=False)
    genes = genes.rename(columns={'genes': 'gene'})

    matrix = pd.read_csv(matrix_filename, sep=' ', usecols=[0,1,2], skiprows=1)

    assert int(matrix.columns[0]) == genes.shape[0]
    assert int(matrix.columns[1]) == cells.shape[0]

    matrix.columns = ['gene_idx', 'cell_idx', 'log_count']
    matrix = matrix.merge(cells[['cell_idx', 'cell_id']])
    matrix = matrix.merge(genes[['gene_idx', 'gene']])

    records = []

    for cell_id, cell_info in matrix.groupby('cell_id'):
        gene_counts = cell_info[['gene', 'log_count']].to_dict(orient='records')
        cell_meta = cells.query(f'cell_id == "{cell_id}"')
        assert cell_meta.shape[0] == 1
        cell_record = cell_meta[['cell_id', 'cell_type', 'UMAP-1', 'UMAP-2', 'sample']].iloc[0].to_dict()
        cell_record['genes'] = gene_counts

        records.append(cell_record)

    load_records('cellgenes', records, host='localhost', port=9200)


directory = '/Users/mcphera1/Scratch/mira_loading'
patient_id = 'TEST'

load_data(directory, patient_id)
