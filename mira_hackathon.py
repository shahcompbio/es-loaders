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


def load_df(matrix):
    records = []

    for cell_id, cell_info in matrix.groupby('cell_id'):
        gene_counts = cell_info[['gene', 'log_count']].to_dict(orient='records')
        cell_meta = cells.query(f'cell_id == "{cell_id}"')
        assert cell_meta.shape[0] == 1
        cell_record = cell_meta[['cell_id', 'cell_type', 'UMAP-1', 'UMAP-2', 'sample']].iloc[0].to_dict()
        cell_record['genes'] = gene_counts

        records.append(cell_record)

    load_records('cellgenes', records, host='localhost', port=9200)


test_matrix = []

def test_load_df(matrix):
    global test_matrix
    test_matrix.append(matrix)


load_df = test_load_df


def load_data(directory, patient_id, chunksize=None):
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

    # Rows and columns are 1-based
    cells['cell_idx'] += 1
    genes['gene_idx'] += 1

    if chunksize is None:
        matrix = pd.read_csv(matrix_filename, sep=' ', usecols=[0,1,2], skiprows=1)

        assert int(matrix.columns[0]) == genes.shape[0]
        assert int(matrix.columns[1]) == cells.shape[0]

        matrix.columns = ['gene_idx', 'cell_idx', 'log_count']
        matrix = matrix.merge(cells[['cell_idx', 'cell_id']])
        matrix = matrix.merge(genes[['gene_idx', 'gene']])

        load_df(matrix)
        return

    prev_chunk = None
    cell_ids = []
    num_records = 0

    matrix_iter = pd.read_csv(matrix_filename, sep=' ', usecols=[0,1,2], skiprows=1, chunksize=chunksize)
    for matrix_chunk in matrix_iter:
        total_cells = int(matrix_chunk.columns[1])
        total_records = int(matrix_chunk.columns[2])

        matrix_chunk.columns = ['gene_idx', 'cell_idx', 'log_count']

        # Need at least 2 cells of data per chunk for correctness of streaming
        if len(matrix_chunk['cell_idx'].unique()) <= 1:
            raise ValueError('chunk size set too low')

        # Identify the last cell to be read
        last_cell_idx = matrix_chunk['cell_idx'].values[-1]

        matrix_chunk = matrix_chunk.merge(cells[['cell_idx', 'cell_id']])
        matrix_chunk = matrix_chunk.merge(genes[['gene_idx', 'gene']])

        # Split out last cell id data
        first_cells_chunk = matrix_chunk.loc[matrix_chunk['cell_idx'] != last_cell_idx]
        last_cell_chunk = matrix_chunk.loc[matrix_chunk['cell_idx'] == last_cell_idx]

        # Merge previous chunk
        if prev_chunk is not None:
            load_chunk = pd.concat([prev_chunk, first_cells_chunk], ignore_index=True)
        else:
            load_chunk = first_cells_chunk

        # Set chunk of last cells aside
        if last_cell_chunk.empty:
            prev_chunk = None
        else:
            prev_chunk = last_cell_chunk

        # Load the data
        load_df(load_chunk)

        # Update for checking later
        cell_ids.append(load_chunk[['cell_id']].drop_duplicates())
        num_records += load_chunk.shape[0]

    if prev_chunk is not None:
        # Load the last cell worth of data
        load_df(prev_chunk)

        # Update for checking later
        cell_ids.append(prev_chunk[['cell_id']].drop_duplicates())
        num_records += prev_chunk.shape[0]

    cell_ids = pd.concat(cell_ids)

    if cell_ids['cell_id'].duplicated().any():
        raise ValueError('streaming failed, duplicate cells')

    num_cells = len(cell_ids['cell_id'].unique())
    if total_cells != num_cells:
        raise ValueError(f'mismatch in {num_cells} cells loaded to {total_cells} total cells')

    if num_records != total_records:
        raise ValueError(f'mismatch in {num_records} loaded to {total_records} total records')


directory = '/Users/mcphera1/Scratch/mira_loading'
patient_id = 'TEST'

load_data(directory, patient_id, chunksize=int(1e6))
test_matrix1 = pd.concat(test_matrix, ignore_index=True)
test_matrix1 = test_matrix1.sort_values(['gene_idx', 'cell_idx']).reset_index(drop=True)

test_matrix = []
load_data(directory, patient_id, chunksize=None)
test_matrix2 = pd.concat(test_matrix, ignore_index=True)
test_matrix2 = test_matrix2.sort_values(['gene_idx', 'cell_idx']).reset_index(drop=True)

print((test_matrix1 == test_matrix2).all().all())

from IPython import embed; embed(); raise
