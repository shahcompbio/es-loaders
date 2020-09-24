from elasticsearch import Elasticsearch
from elasticsearch import helpers

import mira.constants as constants

import logging
logger = logging.getLogger('mira_loading')


def initialize_es(host, port):
    es = Elasticsearch(hosts=[{'host': host, 'port': port}], retry_on_timeout=True, timeout=300)
    return es


def get_bin_sizes(dashboard_id, host, port):
    es = initialize_es(host, port)

    index = constants.DASHBOARD_DATA_PREFIX + dashboard_id.lower()

    query = {
        "size": 0,
        "aggs": {
            "agg_stats_x": {
                "stats": { 
                    "field": "x"
                }
            },
            "agg_stats_y": {
                "stats": {
                    "field": "y"
                }
            }
        }
    }

    result = es.search(index=index, body=query)

    x_stats = result["aggregations"]["agg_stats_x"]
    y_stats = result["aggregations"]["agg_stats_y"]

    ## We assume 100 x 100 bins
    return [ (x_stats["max"] - x_stats["min"]) / 100, (y_stats["max"] - y_stats["min"]) / 100]


def get_genes(host, port):
    es = initialize_es(host, port)

    query = {
        "size": 50000,
        "sort": [
            {
            "gene": {
                "order": "asc"
            }
            }
        ]
    }

    result = es.search(index="genes", body=query)

    return [record["_source"]["gene"] for record in result["hits"]["hits"]]


def msearch(host, port, dashboard_id, query):
    es = initialize_es(host, port)
    index = constants.DASHBOARD_DATA_PREFIX + dashboard_id.lower()

    results = es.msearch(index=index, body=query)

    print(len(results["response"]))

def get_cell_type_count(cell_type, dashboard_id, host, port):
    es = initialize_es(host, port)

    index = constants.DASHBOARD_DATA_PREFIX + dashboard_id.lower()
    
    if not es.indices.exists(index):
        return 0

    result = es.count(index=index, body={
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        "cell_type": cell_type
                    }
                }
            }
        }
    })

    return result["count"]

    

def is_dashboard_loaded(dashboard_id, date, host, port):
    es = initialize_es(host, port)

    if not es.indices.exists(constants.DASHBOARD_ENTRY_INDEX):
        return False
    
    result = es.search(index=constants.DASHBOARD_ENTRY_INDEX, body={
        "query": {
            "bool": {
            "filter": {
                "bool": {
                "must": [
                    {
                    "term": {
                        "dashboard_id": dashboard_id
                    }
                    },   
                    {
                    "range": {
                        "date": {
                        "gte": date
                        }
                    }
                    }
                ]
                }
            }
            }
        }
    })

    return result["hits"]["total"]["value"] > 0 


def query_cell_type_count(dashboard_id, cell_type, host, port):
    es = initialize_es(host, port)
    index = constants.DASHBOARD_DATA_PREFIX + dashboard_id.lower()

    assert es.indices.exists(index)

    query = {
        "query": {
            "bool": {
            "filter": {
                "term": {
                "cell_type": cell_type
                }
            }
            }
        }
    }

    count = es.count(body=query, index=index)

    return count["count"]


## probably want to turn off index refresh here too
def load_cells(records, dashboard_id, host, port):
    load_records(records, constants.DASHBOARD_DATA_PREFIX + dashboard_id.lower(),constants.CELLS_INDEX_MAPPING, host, port)


def load_dashboard_entry(record, dashboard_id, host, port):
    load_record(record, dashboard_id, constants.DASHBOARD_ENTRY_INDEX, constants.DASHBOARD_ENTRY_INDEX_MAPPING, host, port)

def load_rho(records, host, port):
    load_records(records, constants.MARKER_GENES_INDEX, constants.MARKER_GENES_MAPPING, host, port)

def load_genes(records, host, port):
    load_records(records, constants.GENES_INDEX, constants.GENES_MAPPING, host, port)

def load_bins(records, dashboard_id, host, port, refresh=False):
    load_records(records, constants.DASHBOARD_BINS_PREFIX + dashboard_id.lower(), constants.BINS_INDEX_MAPPING, host, port)

    if refresh:
        es = initialize_es(host, port)
        es.indices.refresh(constants.DASHBOARD_BINS_PREFIX + dashboard_id.lower())


def load_records(records, index_name, mapping, host, port):
    es = initialize_es(host, port)

    if not es.indices.exists(index_name):
        logger.info(f'No index found - creating index named {index_name}')
        es.indices.create(
            index=index_name,
            body=mapping
        )
    
    for success, info in helpers.parallel_bulk(es, records, index=index_name):
        if not success:
            # logger.error(info)
            logger.info(info)
            logger.info('Doc failed in parallel loading')

def load_record(record, record_id, index, mapping, host="localhost", port=9200):
    es = initialize_es(host, port)
    if not es.indices.exists(index):
        logger.info(f'No index found - creating index named {index}')
        es.indices.create(index=index, body=mapping)

    logger.info(f'Loading record')
    es.index(index=index, id=record_id, body=record)






#=======


def clean_analysis(dashboard_id, host, port):
    logger.info("====================== " + dashboard_id)
    logger.info("Cleaning records")

    logger.info("DELETE DATA")
    delete_index(constants.DASHBOARD_DATA_PREFIX + dashboard_id.lower(), host=host, port=port)

    delete_index(constants.DASHBOARD_BINS_PREFIX + dashboard_id.lower(), host=host, port=port)

    logger.info("DELETE RHO DATA")
    clean_rho(dashboard_id, host, port)

    logger.info("DELETE DASHBOARD_ENTRY")
    delete_records(constants.DASHBOARD_ENTRY_INDEX, 
                   dashboard_id, host=host, port=port)



def clean_dashboard_entry(dashboard_id, host, port):
    logger.info("DELETE DASHBOARD_ENTRY")
    delete_records(constants.DASHBOARD_ENTRY_INDEX, 
                   dashboard_id, host=host, port=port)


def clean_genes(host, port):
    logger.info("====================== CLEANING GENES")

    logger.info("DELETE GENE LIST")
    delete_index(constants.GENES_INDEX, host=host, port=port)


def clean_rho(dashboard_id, host, port):
    logger.info("====================== CLEANING RHO")

    logger.info("DELETE RHO")
    delete_records(constants.MARKER_GENES_INDEX, dashboard_id, host=host, port=port)


def delete_index(index, host="localhost", port=9200):
    es = initialize_es(host, port)
    if es.indices.exists(index):
        es.indices.delete(index=index, ignore=[400, 404])


def delete_records(index, filter_value, host="localhost", port=9200):
    es = initialize_es(host, port)

    if es.indices.exists(index):
        query = fill_base_query(filter_value)
        es.delete_by_query(index=index, body=query, refresh=True)


def fill_base_query(value):
    return {
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        "dashboard_id": value
                    }
                }
            }
        }
    }

