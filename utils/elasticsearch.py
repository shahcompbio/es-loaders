from elasticsearch import Elasticsearch
from elasticsearch import helpers

import types

import logging
logger = logging.getLogger('mira_loading')


def load_record(index, record, host="localhost", port=9200):
    es = ElasticsearchClient(host=host, port=port)
    if not es.is_index_exists(index):
        es.create_index(index)

    es.es.index(index=index, doc_type="_doc", body=record)

# Putting this method here but later we can extend parallelization through multiple cores by splitting here (as we need multiple ES instances)


def load_records(index, records, host="localhost", port=9200):
    es = ElasticsearchClient(host=host, port=port)

    if not es.is_index_exists(index):
        es.create_index(index)

    if isinstance(records, types.GeneratorType):
        es.load_bulk_parallel(index, records)
    else:
        es.load_bulk(index, records)


class ElasticsearchClient():

    __DEFAULT_SETTINGS = {
        "settings": {
            "index": {
                "max_result_window": 50000
            }
        }
    }

    __DEFAULT_MAPPING = {
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

    # TODO: host + port variables

    def __init__(self, host='localhost', port=9200):
        es = Elasticsearch(hosts=[{'host': host, 'port': port}])

        self.es = es

    # TODO: add settings
    def create_index(self, index):

        self.es.indices.create(
            index=index,
            body={**self.__DEFAULT_SETTINGS, **self.__DEFAULT_MAPPING}
        )

    def is_index_exists(self, index):
        return self.es.indices.exists(index)

    # ###############################
    # LOADING METHODS

    def load_bulk(self, index, records):
        try:
            helpers.bulk(self.es, records, index=index,
                         doc_type="_doc")
        except Exception as e:
            logger.exception("ERROR WHILE LOADING BULK")

    def load_bulk_parallel(self, index, generator):

        for success, info in helpers.parallel_bulk(self.es, generator,
                                                   index=index, doc_type="_doc",
                                                   chunk_size=500):
            if success and logger.isEnabledFor(logging.DEBUG):
                count = self.es.count(index=index)
                logger.debug("Doc count in " + index +
                             " : " + str(count["count"]))

            if not success:
                logger.error(info)
                logger.exception('Doc failed in parallel loading')
