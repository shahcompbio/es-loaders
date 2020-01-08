from elasticsearch import Elasticsearch
import sys

import logging
logger = logging.getLogger('mira_loading')

SAMPLE_METADATA_INDEX = "sample_metadata"
SAMPLE_STATS_INDEX = "sample_stats"
SAMPLE_CELLS_INDEX = "sample_cells"
DASHBOARD_REDIM_INDEX = "dashboard_redim_"
DASHBOARD_GENES_INDEX = "dashboard_genes_"
DASHBOARD_ENTRY_INDEX = "dashboard_entry"
DASHBOARD_CELLS_INDEX = "dashboard_cells"


def clean_analysis(dashboard_id, type, host, port):
    logger.info("====================== " + dashboard_id)
    logger.info("Cleaning records")

    if type == "sample":
        logger.info("DELETE SAMPLE STATS")
        delete_records(SAMPLE_STATS_INDEX, "sample_id",
                       dashboard_id, host=host, port=port)

    logger.info("DELETE DASHBOARD CELLS")
    delete_records(DASHBOARD_CELLS_INDEX, "dashboard_id",
                   dashboard_id, host=host, port=port)

    logger.info("DELETE DASHBOARD GENES")
    delete_index(DASHBOARD_GENES_INDEX +
                 dashboard_id.lower(), host=host, port=port)

    logger.info("DELETE DASHBOARD_ENTRY")
    delete_records(DASHBOARD_ENTRY_INDEX, "dashboard_id",
                   dashboard_id, host=host, port=port)


def delete_index(index, host="localhost", port=9200):
    es = Elasticsearch(hosts=[{'host': host, 'port': port}])
    es.indices.delete(index=index, ignore=[400, 404])


def delete_records(index, filter_key, filter_value, host="localhost", port=9200):
    es = Elasticsearch(hosts=[{'host': host, 'port': port}])
    query = fill_base_query(filter_key, filter_value)
    es.delete_by_query(index=index, body=query)


def fill_base_query(key, value):
    return {
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        key: value
                    }
                }
            }
        }
    }


if __name__ == '__main__':
    pass
