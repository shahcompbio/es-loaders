
import logging
import os
import pandas as pd
import json

from mira.elasticsearch import load_cells, load_bins as _load_bins, load_dashboard_entry as _load_dashboard_entry, load_rho as _load_rho, get_cell_type_count, get_genes, get_bin_sizes, initialize_es
import mira.constants as constants


logger = logging.getLogger('mira_loading')

## main method. 
## Will load both data, metadata, and marker gene records needed for Mira   
## ASSUMES THAT DIRECTORY IS WHERE THE FILES ARE KEPT
## ASSUMES the following files:
##   - genes.tsv
##   - cells.tsv
##   - matrix.mtx
##   - sample_metadata.json
##   - marker_genes.json
def load_analysis(directory, type, dashboard_id, host, port, chunksize=None, metadata={}):
    logger.info("====================== " + dashboard_id)

    load_data(directory, dashboard_id, host, port, chunksize=chunksize, metadata=metadata)
    load_bins(directory, type, dashboard_id, host, port)
    load_rho(directory, dashboard_id, host, port)
    load_dashboard_entry(directory, type,dashboard_id, metadata, host, port)

    logger.info("Done.")
    


def load_rho(directory, dashboard_id, host, port):
    logger.info("LOADING MARKER GENES: " + dashboard_id)

    logger.debug("Opening files")
    markers_filename = os.path.join(directory, constants.MARKER_GENES_FILENAME)

    with open(markers_filename) as markers_file:
        cell_types = json.load(markers_file)


    logger.debug("Processing files")
    records = []

    for cell_type_record in cell_types:
        count = get_cell_type_count(cell_type_record["cell_type"], dashboard_id, host, port)

        records.append({
            **cell_type_record,
            "dashboard_id": dashboard_id,
            "count": count
        })        

    _load_rho(records, host=host, port=port)
    logger.info("LOADED MARKER GENES")


## Loading metadata for Mira
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


def load_data(directory, dashboard_id, host, port, chunksize=None, metadata={}):
    logger.info("LOADING DATA: " + dashboard_id)

    logger.debug("Opening files")

    cells_filename = os.path.join(directory, constants.CELLS_FILENAME)
    genes_filename = os.path.join(directory, constants.GENES_FILENAME)
    matrix_filename = os.path.join(directory, constants.MATRIX_FILENAME)
    metadata_filename = os.path.join(directory, constants.SAMPLES_FILENAME)

    logger.info("Opening Files at: " + directory)
    logger.info("Opening cell file")
    cells = pd.read_csv(cells_filename, sep='\t')

    if 'cell_id' not in cells.columns:
        cells.index.name = 'cell_id'
        cells = cells.reset_index(drop=False)
        
    if 'cell_idx' not in cells.columns:
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
    cells = cells.rename(columns={'sample':'sample_id', 'UMAP-1': 'x', 'UMAP-2': 'y', 'umap50_1': 'x', 'umap50_2': 'y', "UMAP_1": "x", "UMAP_2": "y", "umapharmony_1": 'x', 'umapharmony_2': 'y'})
    cells = cells.merge(samples, on='sample_id', how='left')

    ## Check that all columns are there
    column_names = list(cells.columns)
    assert 'x' in column_names and 'y' in column_names, 'Missing x and y'
    assert 'cell_id' in column_names, 'Missing cell ID'
    assert 'cell_idx' in column_names, 'Missing cell idx'
    assert 'sample_id' in column_names, 'Missing sample ID'


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

        logger.info(f'Loading {matrix.shape[0]} records with total {cells.shape[0]} cells ({round(cells.shape[0] * 100 / before_cell_count, 2)}%) and {matrix.shape[0]} gene records')
        
        load_cells(get_records(cells, matrix), dashboard_id, host, port)
        return

    prev_chunk = None
    cell_ids = []
    cell_count = 0
    num_records = 0

    logger.info("Starting to chunk matrix file")

    matrix_iter = pd.read_csv(matrix_filename, sep=' ', usecols=[0,1,2], skiprows=1, chunksize=chunksize)

    for matrix_chunk in matrix_iter:
        total_cells = int(cells.shape[0])
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

        # Load the data if there are records
        if load_chunk.shape[0] > 0:
            logger.info(f'Loading {load_chunk.shape[0]} records with total {cell_count} cells ({round(cell_count * 100/ total_cells, 2)}%) and {num_records} gene records')
            load_cells(get_records(cells, load_chunk), dashboard_id, host, port)
 
    # Clear queue
    if prev_chunk is not None:
        cell_ids.append(prev_chunk[['cell_id']].drop_duplicates())
        num_records += prev_chunk.shape[0]
        cell_count += len(prev_chunk['cell_id'].unique())

        logger.info(f'Loading {prev_chunk.shape[0]} records with total {cell_count} cells ({round(cell_count * 100/ total_cells, 2)}%) and {num_records} gene records')

        # Load the last cell worth of data
        load_cells(get_records(cells, prev_chunk), dashboard_id, host, port)


    cell_ids = pd.concat(cell_ids)

    if cell_ids['cell_id'].duplicated().any():
        raise ValueError('streaming failed, duplicate cells')
        ## !!! wilL need to eventually delete or something

    num_cells = len(cell_ids['cell_id'].unique())
    if total_cells != num_cells:
        raise ValueError(f'mismatch in {num_cells} cells loaded to {total_cells} total cells')


def get_records(cells, matrix):


    ### !!! remember to remove
    logger.info(f"matrix before filter: {matrix.shape[0]}")
    matrix = matrix[matrix["gene_idx"] < 10000]
    logger.info(f"matrix after filter: {matrix.shape[0]}")

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


def load_bins(directory, type, dashboard_id, host, port):
    logger.info("LOAD BINS: " + dashboard_id)

    [x_bin_size, y_bin_size] = get_bin_sizes(dashboard_id, host, port)
    es = initialize_es(host, port)
    index = constants.DASHBOARD_DATA_PREFIX + dashboard_id.lower()


    logger.info("categorical")
    categorical_labels = ["cell_type", "surgery", "site", "therapy", "sort", "sample_id"]

    if type == "cohort" and dashboard_id != "cohort_all":
        categorical_labels.append("cluster_label")

    data_header = json.dumps({})
    data_str = ''

    for label in categorical_labels:
        data_str += data_header + '\n' + json.dumps({"size":0,      "aggs": {
            "agg_histogram_x": {
                "histogram": {
                    "field": "x",
                    "interval": x_bin_size,
                    "min_doc_count": 1
                },
                "aggs": {
                    "agg_histogram_y": {
                    "histogram": {
                        "field": "y",
                        "interval": y_bin_size,
                        "min_doc_count": 1
                    },
                    "aggs": {
                        "agg_cat": {
                            "terms": {
                                "field": label,
                                "size": 1
                            }
                        }
                        
                    }
                    }
                }
            }
        }}) + '\n'
    
    logger.info("querying")
    results = es.msearch(index=index, body=data_str)
    logger.info(f'queries results: {len(results["responses"])}')

    processed_records = []
    for idx, res_chunk in enumerate(results["responses"]):
        if 'error' in res_chunk.keys():
            logger.info(categorical_labels[idx])
            logger.info(res_chunk['error'])
        else:
            for response_x in res_chunk["aggregations"]["agg_histogram_x"]["buckets"]:
                for response_y in response_x["agg_histogram_y"]["buckets"]:
                    processed_record = {
                        "x": round(response_x["key"] / x_bin_size),
                        "y": round(response_y["key"] / y_bin_size),
                        "count": response_y["doc_count"],
                        "label": categorical_labels[idx],
                        "value": response_y["agg_cat"]["buckets"][0]["key"]
                    }

                    processed_records.append(processed_record)



    logger.info(f'records: {len(processed_records)}')


    _load_bins(processed_records, dashboard_id, host, port)

    logger.info("genes")


    cells_filename = os.path.join(directory, constants.CELLS_FILENAME)
    genes_filename = os.path.join(directory, constants.GENES_FILENAME)
    matrix_filename = os.path.join(directory, constants.MATRIX_FILENAME)

    logger.info("Opening Files at: " + directory)
    logger.info("Opening cell file")
    cells = pd.read_csv(cells_filename, sep='\t')

    if 'cell_id' not in cells.columns:
        cells.index.name = 'cell_id'
        cells = cells.reset_index(drop=False)
        
    if 'cell_idx' not in cells.columns:
        cells.index.name = 'cell_idx'
        cells = cells.reset_index(drop=False)

    cells['cell_type'] = cells['cell_type'].str.replace('.', ' ')
    cells = cells.rename(columns={'sample':'sample_id', 'UMAP-1': 'x', 'UMAP-2': 'y', 'umap50_1': 'x', 'umap50_2': 'y', "UMAP_1": "x", "UMAP_2": "y", "umapharmony_1": 'x', 'umapharmony_2': 'y'})

    cells['x'] = cells['x'] // x_bin_size
    cells['y'] = cells['y'] // y_bin_size

    ## convert to dict???
    cells['count'] = 0
    binned_counts = cells[['x', 'y','count']].groupby(['x','y']).count().reset_index().to_dict(orient='record')

    logger.info("Opening genes file")
    genes = pd.read_csv(genes_filename, sep='\t')
    genes.index.name = 'gene_idx'
    genes = genes.reset_index(drop=False)
    genes = genes.rename(columns={'genes': 'gene'})

    # Rows and columns are 1-based
    cells['cell_idx'] += 1
    genes['gene_idx'] += 1



    matrix_iter = pd.read_csv(matrix_filename, sep=' ', usecols=[0,1,2], skiprows=1, chunksize=int(1e6))

    count_bins = {}
    for bin_count in binned_counts:
        key = f"{bin_count['x']}_{bin_count['y']}"
        count_bins[key] = {'count': bin_count['count']}

    logger.info(f'Bins: {len(count_bins.keys())}')

    num_chunk = 0
    for matrix_chunk in matrix_iter:
        logger.info(f'processing chunk {num_chunk} ')
        matrix_chunk.columns = ['gene_idx', 'cell_idx', 'log_count']

        matrix_chunk = matrix_chunk.merge(cells[['cell_idx', 'x', 'y']])
        matrix_chunk = matrix_chunk.merge(genes[['gene_idx', 'gene']])

        counts = matrix_chunk[['gene', 'log_count', 'x', 'y']].to_dict(orient="records")

        for count in counts:
            key = f"{count['x']}_{count['y']}"
            curr_bin = count_bins[key]
            curr_gene = count['gene']
            if curr_gene in curr_bin:
                curr_bin[curr_gene] = curr_bin[curr_gene] + count['log_count']
            else:
                curr_bin[curr_gene] = count['log_count']

            count_bins[key] = curr_bin

        num_chunk += 1



    records = []
    total_records = 0

    gene_names = genes['gene'].to_list()

    for key, counts in count_bins.items():
        [x, y] = key.split("_")
        
        total_count = counts['count']

        for gene in gene_names:
            record = {
                'x': float(x),
                'y': float(y),
                'count': total_count,
                'label': gene,
                'value': counts[gene] / total_count if gene in counts else 0
            }

            records.append(record)

        if len(records) > int(1e6):
            logger.info(f'Records: {len(records)}')
            logger.info(f'Example record: {records[0]}')
            _load_bins(records, dashboard_id, host, port)

            total_records += len(records)
            logger.info(f'Total records: {total_records}')

            records = []

    if len(records) > 0:
        logger.info(f'Records: {len(records)}')
        logger.info(f'Example record: {records[0]}')
        _load_bins(records, dashboard_id, host, port, refresh=True)

        total_records += len(records)
        logger.info(f'Total records: {total_records}')

