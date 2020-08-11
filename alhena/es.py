from elasticsearch import Elasticsearch
from elasticsearch import helpers
import os

import logging

import urllib3
urllib3.disable_warnings()


DEFAULT_MAPPING = {
    "settings": {
        "index": {
            "max_result_window": 50000
        }
    },
    'mappings': {
        "dynamic_templates": [
            {
                "string_values": {
                    "match": "*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword"
                    }
                }
            }
        ]
    }
}


def initialize_es(host, port):
    assert os.environ['ALHENA_ES_USER'] is not None and os.environ['ALHENA_ES_PASSWORD'] is not None, 'Elasticsearch credentials missing'

    es = Elasticsearch(hosts=[{'host': host, 'port': port}], 
        http_auth=(os.environ['ALHENA_ES_USER'], os.environ['ALHENA_ES_PASSWORD']),
        scheme='https',
        timeout=300, 
        verify_certs=False)
        
    return es



def load_records(records, index_name, host, port, mapping=DEFAULT_MAPPING):
    es = initialize_es(host, port)

    if not es.indices.exists(index_name):
        logging.info(f'No index found - creating index named {index_name}')
        es.indices.create(
            index=index_name,
            body=mapping
        )
    
    for success, info in helpers.parallel_bulk(es, records, index=index_name):
        if not success:
            # logger.error(info)
            logging.info(info)
            logging.info('Doc failed in parallel loading')

def load_record(record, record_id, index, host, port, mapping=DEFAULT_MAPPING):
    es = initialize_es(host, port)
    if not es.indices.exists(index):
        logging.info(f'No index found - creating index named {index}')
        es.indices.create(index=index, body=mapping)

    logging.info(f'Loading record')
    es.index(index=index, id=record_id, body=record)

