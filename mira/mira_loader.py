
import logging
import os
import pandas as pd

from mira.elasticsearch import load_cells, load_dashboard_entry as _load_dashboard_entry, load_rho as _load_rho
import mira.constants as constants
from mira.rho_loader import download_rho_data, generate_dashboard_rho


logger = logging.getLogger('mira_loading')




## main method. Will load both data and metadata records needed for Mira
def load_analysis(directory, type, dashboard_id, host, port, isCohort=False, chunksize=None, metadata={}):
    logger.info("====================== " + dashboard_id)
    load_data(directory, dashboard_id, host, port,isCohort=isCohort, chunksize=chunksize, metadata=metadata)

    load_rho(dashboard_id, host, port)
    load_dashboard_entry(directory, type,dashboard_id, metadata, host, port)


    logger.info("Done.")
    


def load_rho(dashboard_id, host, port):
    data = download_rho_data()
    data = generate_dashboard_rho(data, dashboard_id)
    _load_rho(data, host=host, port=port)


## Loading metadata for Mira
## Assumes samples_metadata.json file exists
def load_dashboard_entry(directory, type, dashboard_id, dashboard_metadata, host, port):

    logger.info("LOADING DASHBOARD ENTRY: " + dashboard_id)

    logger.debug("Opening files")
    metadata_filename = os.path.join(directory, constants.SAMPLES_FILENAME)

    with open(metadata_filename) as samples_file:
        samples = pd.read_json(samples_file)

    record = {
        "dashboard_id": dashboard_id,
        "type": type,
        "samples": samples[:].to_dict(orient='records'),
        **dashboard_metadata
    }

    _load_dashboard_entry(record, dashboard_id, host, port)
    logger.info("LOADED DASHBOARD ENTRY")


def load_data(directory, dashboard_id, host, port, isCohort=False, chunksize=None, metadata={}):
    logger.info("LOADING DATA: " + dashboard_id)

    logger.debug("Opening files")
    if isCohort:
        if dashboard_id == "cohort_all":
            cells_filename = os.path.join(directory, constants.CELLS_FILENAME)
        else:
            cells_filename = os.path.join(directory, dashboard_id + "_cells.tsv")
    else:
        cells_filename = os.path.join(directory, constants.CELLS_FILENAME)
    genes_filename = os.path.join(directory, constants.GENES_FILENAME)
    matrix_filename = os.path.join(directory, constants.MATRIX_FILENAME)
    metadata_filename = os.path.join(directory, constants.SAMPLES_FILENAME)

    logger.info("Opening Files at: " + directory)
    logger.info("Opening cell file")
    cells = pd.read_csv(cells_filename, sep='\t')
    if not isCohort:
        cells.index.name = 'cell_id'
        cells = cells.reset_index(drop=False)
    cells.index.name = 'cell_idx'
    cells = cells.reset_index(drop=False)
    cells['cell_type'] = cells['cell_type'].str.replace('.', ' ')

    logger.info("Opening genes file")
    genes = pd.read_csv(genes_filename, sep='\t')
    genes.index.name = 'gene_idx'
    genes = genes.reset_index(drop=False)
    genes = genes.rename(columns={'genes': 'gene'})

    # Rows and columns are 1-based
    cells['cell_idx'] += 1
    genes['gene_idx'] += 1


    logger.info("Opening metadata file")
    with open(metadata_filename) as samples_file:
        samples = pd.read_json(samples_file)

    before_cell_count = cells.shape[0]
    cells = cells.rename(columns={'sample':'sample_id', 'UMAP-1': 'x', 'UMAP-2': 'y', 'umap50_1': 'x', 'umap50_2': 'y', "UMAP_1": "x", "UMAP_2": "y"})
    cells = cells.merge(samples, on='sample_id', how='left')

    # Sanity check that joining with sample table didn't delete cell entries
    assert before_cell_count == cells.shape[0]

    logger.info("Cells: " + str(cells.shape[0]))
    logger.info("Genes: " + str(genes.shape[0]))
    logger.info("Samples: " + str(samples.shape[0]))

    if chunksize is None:
        matrix = pd.read_csv(matrix_filename, sep=' ', usecols=[0,1,2], skiprows=1)

        assert int(matrix.columns[0]) == genes.shape[0]
        assert int(matrix.columns[1]) == cells.shape[0]

        matrix.columns = ['gene_idx', 'cell_idx', 'log_count']
        matrix = matrix.merge(cells[['cell_idx', 'cell_id']])
        matrix = matrix.merge(genes[['gene_idx', 'gene']])
        matrix = matrix.merge(samples[['sample_id']])

        logger.info(f'Loading {matrix.shape[0]} records with total {cells.shape[0]} cells and {matrix.shape[0]} gene records')
        
        load_cells(get_records(cells, matrix), dashboard_id, host, port)
        return

    prev_chunk = None
    cell_ids = []
    cell_count = 0
    num_records = 0

    logger.info("Starting to chunk matrix file")

    matrix_iter = pd.read_csv(matrix_filename, sep=' ', usecols=[0,1,2], skiprows=1, chunksize=chunksize)

    for matrix_chunk in matrix_iter:
        logger.info(f'Matrix size: {matrix_chunk.shape}')
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
        # matrix_chunk = matrix_chunk.merge(samples[['sample_id']])

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

        # Update for checking later
        cell_ids.append(load_chunk[['cell_id']].drop_duplicates())
        cell_count += len(load_chunk['cell_id'].unique())
        num_records += load_chunk.shape[0]

        # Load the data
        logger.info(f'Loading {load_chunk.shape[0]} records with total {cell_count} cells and {num_records} gene records')
        load_cells(get_records(cells, load_chunk), dashboard_id, host, port)
 
    # Clear queue
    if prev_chunk is not None:
        logger.info("END")
        cell_ids.append(prev_chunk[['cell_id']].drop_duplicates())
        num_records += prev_chunk.shape[0]
        cell_count += len(prev_chunk['cell_id'].unique())

        logger.info(f'Loading {prev_chunk.shape[0]} records with total {cell_count} cells and {num_records} gene records')

        # Load the last cell worth of data
        load_cells(get_records(cells, prev_chunk), dashboard_id, host, port)


    cell_ids = pd.concat(cell_ids)

    if cell_ids['cell_id'].duplicated().any():
        raise ValueError('streaming failed, duplicate cells')
        ## !!! wilL need to eventually delete or something

    num_cells = len(cell_ids['cell_id'].unique())
    if total_cells != num_cells:
        raise ValueError(f'mismatch in {num_cells} cells loaded to {total_cells} total cells')

    if num_records != total_records:
        raise ValueError(f'mismatch in {num_records} loaded to {total_records} total records')


def get_records(cells, matrix):
    records = []
    for cell_id, cell_info in matrix.groupby('cell_id'):
        gene_counts = cell_info[['gene', 'log_count']].to_dict(orient='records')
        cell_meta = cells.query(f'cell_id == "{cell_id}"')
        assert cell_meta.shape[0] == 1
        # cell_record = cell_meta[['cell_id', 'cell_type', 'UMAP-1', 'UMAP-2', 'sample']].iloc[0].to_dict()
        cell_record = cell_meta[:].iloc[0].to_dict()
        cell_record['genes'] = gene_counts

        records.append(cell_record)

    return records


## main method. Will load both data and metadata records needed for Mira
# def load_analysis(directory, dashboard_id, host, port, isCohort=False, chunksize=None, metadata={}):
#     logger.info("====================== " + dashboard_id)
#     load_data(directory, dashboard_id, host, port,isCohort=isCohort, chunksize=chunksize, metadata=metadata)

#     load_dashboard_entry(directory, dashboard_id, metadata, host, port)


#     logger.info("Done.")
    


def load_celltype_data(directory, dashboard_id, host, port, chunksize=None, metadata={}):
    logger.info("LOADING DATA: " + dashboard_id)

    logger.debug("Opening files")
    cells_filename = os.path.join(directory, dashboard_id + "_cells.tsv")
    
    cohort_cells_filename = os.path.join(directory, constants.CELLS_FILENAME)
    genes_filename = os.path.join(directory, constants.GENES_FILENAME)
    matrix_filename = os.path.join(directory, constants.MATRIX_FILENAME)
    metadata_filename = os.path.join(directory, constants.SAMPLES_FILENAME)

    logger.info("Opening Files at: " + directory)

    cells = pd.read_csv(cells_filename, sep='\t')
    # if not isCohort:
    #     cells.index.name = 'cell_id'
    #     cells = cells.reset_index(drop=False)
    # cells.index.name = 'cell_idx'
    # cells = cells.reset_index(drop=False)
    cells['cell_type'] = cells['cell_type'].str.replace('.', ' ')

    cohort_cells = pd.read_csv(cohort_cells_filename, sep='\t')
    cohort_cells.index.name = 'cell_idx'
    cohort_cells = cohort_cells.reset_index(drop=False)
    cohort_cells = cohort_cells[['cell_idx', 'cell_id']]
    cells = cells.merge(cohort_cells[['cell_idx', 'cell_id']])

    logger.info("Opening genes file")
    genes = pd.read_csv(genes_filename, sep='\t')
    genes.index.name = 'gene_idx'
    genes = genes.reset_index(drop=False)
    genes = genes.rename(columns={'genes': 'gene'})

    # Rows and columns are 1-based
    cells['cell_idx'] += 1
    genes['gene_idx'] += 1


    logger.info("Opening metadata file")
    with open(metadata_filename) as samples_file:
        samples = pd.read_json(samples_file)

    before_cell_count = cells.shape[0]
    cells = cells.rename(columns={'sample':'sample_id', 'UMAP-1': 'x', 'UMAP-2': 'y', 'umap50_1': 'x', 'umap50_2': 'y', "UMAP_1": "x", "UMAP_2": "y"})
    cells = cells.merge(samples, on='sample_id', how='left')

    # Sanity check that joining with sample table didn't delete cell entries
    assert before_cell_count == cells.shape[0]

    logger.info("Cells: " + str(cells.shape[0]))
    logger.info("Genes: " + str(genes.shape[0]))
    logger.info("Samples: " + str(samples.shape[0]))

    if chunksize is None:
        matrix = pd.read_csv(matrix_filename, sep=' ', usecols=[0,1,2], skiprows=1)

        assert int(matrix.columns[0]) == genes.shape[0]
        assert int(matrix.columns[1]) == cells.shape[0]

        matrix.columns = ['gene_idx', 'cell_idx', 'log_count']
        matrix = matrix.merge(cells[['cell_idx', 'cell_id']])
        matrix = matrix.merge(genes[['gene_idx', 'gene']])
        matrix = matrix.merge(samples[['sample_id']])

        logger.info(f'Loading {matrix.shape[0]} records with total {cells.shape[0]} cells and {matrix.shape[0]} gene records')
        
        load_cells(get_records(cells, matrix), dashboard_id, host, port)
        return

    prev_chunk = None
    cell_ids = []
    cell_count = 0
    num_records = 0

    logger.info("Starting to chunk matrix file")


    total_cells = int(cells.shape[0])

    matrix_iter = pd.read_csv(matrix_filename, sep=' ', usecols=[0,1,2], skiprows=1, chunksize=chunksize)

    for matrix_chunk in matrix_iter:

        matrix_chunk.columns = ['gene_idx', 'cell_idx', 'log_count']

        # Need at least 2 cells of data per chunk for correctness of streaming
        if len(matrix_chunk['cell_idx'].unique()) <= 1:
            raise ValueError('chunk size set too low')

        # Identify the last cell to be read
        last_cell_idx = matrix_chunk['cell_idx'].values[-1]

        matrix_chunk = matrix_chunk.merge(cells[['cell_idx', 'cell_id']])
        matrix_chunk = matrix_chunk.merge(genes[['gene_idx', 'gene']])
        # matrix_chunk = matrix_chunk.merge(samples[['sample_id']])

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

        # Update for checking later
        cell_ids.append(load_chunk[['cell_id']].drop_duplicates())
        cell_count += len(load_chunk['cell_id'].unique())
        num_records += load_chunk.shape[0]

        # Load the data if there are records
        if load_chunk.shape[0] > 0:
            logger.info(f'Loading {load_chunk.shape[0]} records with total {cell_count} cells and {num_records} gene records')
            load_cells(get_records(cells, load_chunk), dashboard_id, host, port)
 
    # Clear queue
    if prev_chunk is not None:
        cell_ids.append(prev_chunk[['cell_id']].drop_duplicates())
        num_records += prev_chunk.shape[0]
        cell_count += len(prev_chunk['cell_id'].unique())

        logger.info(f'Loading {prev_chunk.shape[0]} records with total {cell_count} cells and {num_records} gene records')

        # Load the last cell worth of data
        load_cells(get_records(cells, prev_chunk), dashboard_id, host, port)


    cell_ids = pd.concat(cell_ids)

    if cell_ids['cell_id'].duplicated().any():
        raise ValueError('streaming failed, duplicate cells')
        ## !!! wilL need to eventually delete or something

    num_cells = len(cell_ids['cell_id'].unique())
    if total_cells != num_cells:
        raise ValueError(f'mismatch in {num_cells} cells loaded to {total_cells} total cells')

    # if num_records != total_records:
    #     raise ValueError(f'mismatch in {num_records} loaded to {total_records} total records')

    load_dashboard_entry(directory, "cohort", dashboard_id, metadata, host, port)


# load_analysis('~/data/003/', "003", 'plvicosspecdat2', 9200)
# load_dashboard_entry('/data/mira/SPECTRUM-OV-070', 'SPECTRUM-OV-070', { "date": "103008"}, 'plvicosspecdat2', 9200)