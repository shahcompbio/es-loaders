import os
import sys
import json
import click
import logging
import collections
import math
import scipy.stats
import pandas as pd
import numpy as np
from esclient import ElasticsearchClient
from scgenome.loaders.qc import load_qc_data
from scgenome.db.qc import cache_qc_results
from analysis_loader import AnalysisLoader

"""
Eli Havasov 1-27-2021
loadalhena.py is based off of loadmontage.py from 
https://github.com/shahcompbio/montageloader2.

Alhena is the new version of Montage. Alhena uses ElasticSearch version 7+
whereas Montage does not. Newer version does not support document mappings
so code changes were necessary for Alhena.

Montage had an index per ticket  with 4 document types (segs, bins, qc 
& gc_bias), whereas Alhena has 4 different indexes per ticket for the
different types.

esclient.py is also slightly modified to accodmondate for the changes
made in this script.
"""

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


caller_map = {
    'segs': 'single_cell_hmmcopy_seg',
    'bins': 'single_cell_hmmcopy_bin',
    'qc': 'single_cell_qc',
    'gc_bias': 'single_cell_gc_bias',
}


chr_prefixed = {str(a): '0' + str(a) for a in range(1, 10)}


def init_load(elasticsearch_client, index_name,):
    if elasticsearch_client.is_index_exists(index_name):
        elasticsearch_client.delete_index(index_name)

    elasticsearch_client.create_index(index_name)


def clean_fields(data):
    invalid_chars = ['.']
    invalid_cols = [col for col in data.columns if any(
        [char in col for char in invalid_chars])]
    for col in invalid_cols:
        found_chars = [char for char in invalid_chars if char in col]

        for char in found_chars:
            new_col = col.replace(char, '_')
            data.rename(columns={col: new_col}, inplace=True)


def clean_nans(record):
    floats = [field for field in record if isinstance(record[field], float)]
    for field in floats:
        if np.isnan(record[field]):
            del record[field]


def load_index(elasticsearch_client, index_name, data,):
    data_header = json.dumps({'index': {'_index': index_name}})

    batch_size = int(1e4)
    for batch_start_idx in range(0, data.shape[0], batch_size):
        batch_end_idx = min(batch_start_idx + batch_size, data.shape[0])
        batch_data = data.loc[data.index[batch_start_idx:batch_end_idx]]

        clean_fields(batch_data)

        data_str = ''
        for record in batch_data.to_dict(orient='records'):
            clean_nans(record)
            data_str += data_header + '\n' + json.dumps(record) + '\n'

        count_before = elasticsearch_client.count(index_name)
        elasticsearch_client.load_bulk(index_name, data_str)
        count_after = elasticsearch_client.count(index_name)
        count_added = count_after - count_before

        logging.info(f'Count before: {count_before}')
        logging.info(f'Count after: {count_after}')
        logging.info(f'Count added: {count_added}')

        if count_added != batch_data.shape[0]:
            raise Exception(
                f'expected to add {batch_data.shape[0]} but added {count_added}')


def create_chrom_number(chromosomes):
    chrom_number = chromosomes.map(lambda a: chr_prefixed.get(a, a))
    return chrom_number


def filter_by_sample_id(hmmcopy_data, sample_id):
    for table_name, data in hmmcopy_data.items():
        if 'sample_id' not in data.columns:
            raise ValueError(f'sample id not in table {table_name}')
        hmmcopy_data[table_name] = data[data['sample_id'] == sample_id]


def get_qc_data(hmmcopy_data):
    data = hmmcopy_data['annotation_metrics']
    data['percent_unmapped_reads'] = data["unmapped_reads"] / data["total_reads"]
    data['is_contaminated'] = data['is_contaminated'].apply(
        lambda a: {True: 'true', False: 'false'}[a])
    return data


def get_segs_data(hmmcopy_data):
    data = hmmcopy_data['hmmcopy_segs'].copy()
    data['chrom_number'] = create_chrom_number(data['chr'])
    return data


def get_bins_data(hmmcopy_data):
    data = hmmcopy_data['hmmcopy_reads'].copy()
    data['chrom_number'] = create_chrom_number(data['chr'])
    return data


def get_gc_bias_data(hmmcopy_data):
    data = hmmcopy_data['gc_metrics']
    # data = data.merge(hmmcopy_data['annotation_metrics'][[
    #                   'cell_id', 'experimental_condition']], on='cell_id', how='left')

    # # Create a stacked data frame with columns:
    # # experimental_condition, gc_percent, value
    # gc_percent_columns = [str(a) for a in range(101)]
    # data = data.set_index('experimental_condition')[gc_percent_columns]
    # data.columns.name = 'gc_percent'
    # data = data.stack().rename('value').reset_index()

    # # Calculate stats for groups by experimental condition
    # data = data.groupby(['experimental_condition', 'gc_percent'])['value'].agg([
    #     'mean', 'median', 'std', 'size'])

    # # Add 95% confidence intervals
    # t_bounds = scipy.stats.t.interval(0.95, data['size'] - 1)
    # data['low_ci'] = data['mean'] + t_bounds[0] * \
    #     data['std'] / np.sqrt(data['size'])
    # data['high_ci'] = data['mean'] + t_bounds[1] * \
    #     data['std'] / np.sqrt(data['size'])

    # columns = ['experimental_condition',
    #            'gc_percent', 'high_ci', 'low_ci', 'median']
    # data = data.reset_index()[columns]

    return data


@click.command()
@click.argument('jira_ticket')
@click.argument('ip_address')
@click.option('--local_cache_directory')
@click.option('--ticket_directory', multiple=True)
@click.option('--description', default=None)
@click.option('--sample_id', default=None)
@click.option('--library_id', default=None)
@click.option('--cell_subset_count', default=None, type=int)
@click.option('--cell_ids', '-c', multiple=True)
@click.option('--experimental_condition_override')
def load_ticket(
    jira_ticket,
    ip_address,
    local_cache_directory=None,
    ticket_directory=None,
    description=None,
    sample_id=None,
    library_id=None,
    cell_subset_count=None,
    cell_ids=None,
    experimental_condition_override=None,
):

    if (local_cache_directory is not None) == (len(ticket_directory) > 0):
        raise ValueError(
            'must specify one of local_cache_directory or ticket_directory')

    if len(cell_ids) == 0:
        cell_ids = None

    if cell_subset_count and cell_ids:
        logging.info(
            f'Sorry, --cell_subset_count and --cell_ids arguments cannot be used together')
        return

    logging.info(f'jira ticket {jira_ticket}')

    if local_cache_directory is not None:
        cache_qc_results(jira_ticket, local_cache_directory)
        ticket_directory = [os.path.join(local_cache_directory, jira_ticket)]

    hmmcopy_data = collections.defaultdict(list)
    for d in ticket_directory:
        for table_name, data in load_qc_data(d).items():
            hmmcopy_data[table_name].append(data)
    for table_name in hmmcopy_data:
        hmmcopy_data[table_name] = pd.concat(
            hmmcopy_data[table_name], ignore_index=True)

    if experimental_condition_override is not None:
        for table_name, data in hmmcopy_data.items():
            if 'experimental_condition' in data:
                data['experimental_condition'] = experimental_condition_override

    logging.info(f'loading hmmcopy data with tables {hmmcopy_data.keys()}')

    # if sample_id is not None:
    #     logging.info(f'filtering hmmcopy data by sample={sample_id}')
    #     filter_by_sample_id(hmmcopy_data, sample_id)

    elasticsearch_client = ElasticsearchClient(host=ip_address)

    if cell_subset_count is not None:
        cell_ids = hmmcopy_data['annotation_metrics']['cell_id'].iloc[:cell_subset_count].values

    index = jira_ticket.lower()
    index_get_data = {
        f"qc": get_qc_data,
        f"segs": get_segs_data,
        f"bins": get_bins_data,
        f"gc_bias": get_gc_bias_data,
    }

    for index_type, get_data in index_get_data.items():
        index_name = f"{jira_ticket.lower()}_{index_type}"
        logging.info(f"Index {index_name}")

        init_load(elasticsearch_client, index_name,)

        data = get_data(hmmcopy_data)

        # Subset cells
        if cell_ids is not None and index_type != 'gc_bias':
            data = data[data['cell_id'].isin(cell_ids)]

        logging.info(f"dataframe for {index_name} has shape {data.shape}")

        load_index(elasticsearch_client, index_name, data,)

    logging.info(
        f"loading published dashboard record {jira_ticket}")

    if description is not None:
        # Assuming not colossus so need to check that all the other fields are there
        assert sample_id is not None, "Must specify sample_id"
        assert library_id is not None, "Must specify library_id"

        analysis_record = {
            "sample_id": sample_id,
            "library_id": library_id,
            "jira_id": jira_ticket,
            "description": description
        }
        elasticsearch_client.load_record(record, self."analyses", jira_ticket)

    else:
        AnalysisLoader().load_data(jira_ticket, ip_address, 9200)

# elasticsearch_client.load_published_dashboard_record(
#     jira_ticket, title=title, description=description)


def json_to_dict(path):
    with open(path) as json_file:
        return json.load(json_file)


def run_test_load():
    elasticsearch_client = ElasticsearchClient(host='localhost')
    ticket = 'test'

    for index_type in ('qc', 'segs', 'bins', 'gc_bias'):

        data = json_to_dict(f'./meta/{index_type}.json')
        data = pd.DataFrame(data)
        index_name = f"{ticket}_{index_type}"

        init_load(elasticsearch_client, index_name,)
        load_index(elasticsearch_client, index_name, data,)

    elasticsearch_client.load_published_dashboard_record(
        ticket, title='test description', description='test description')


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT,
                        stream=sys.stderr, level=logging.INFO)

    load_ticket()
    # run_test_load()
