from elasticsearch import Elasticsearch
from elasticsearch import helpers

import types


settings = {
    'settings': {
        'index': {
            'number_of_replicas': 0,
            'max_result_window': 500000,
            'number_of_shards': 3
        }
    }
}


default_mapping = {
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


# TODO: would be more useful if this worked at a higher
# level, perhaps taking dataframes as input and doing a bulk
# load rather than copying all elasticsearch methods


class ElasticsearchClient():

    def __init__(self, host='localhost', port=9200):
        es = Elasticsearch(hosts=[{'host': host, 'port': port}], timeout=300)
        self.es = es

    def create_index(self, index):
        self.es.indices.create(
            index=index,
            body={**settings, **default_mapping}
        )

    def is_index_exists(self, index):
        return self.es.indices.exists(index)

    def search(self, index, doc_type, query):
        result = self.es.search(
            index=index,
            doc_type=doc_type,
            body=query)

        return result

    def count(self, index):
        return self.es.count(index=index)['count']

    def put_mapping(self, index, doc_type, body):
        self.es.indices.put_mapping(index=index, doc_type=doc_type, body=body)

    def load_bulk(self, index, body):
        self.es.bulk(refresh='true', index=index, body=body)

    def load_record(self, record, index, id):
        if not self.is_index_exists(index):
            self.create_index(index)

        # TODO: is_record_exists always true and then delete fails...
        # if not self.is_record_exists(index, doc, id):
        #    self.es.delete(index=index, doc_type=doc, id=id)

        self.es.index(index=index, id=id, body=record)

    def is_record_exists(self, index, doc, id):
        return self.es.exists(index=index, doc_type=doc, id=id)

    def load_published_dashboard_record(self, jira_id, description='', title=''):
        record = {
            "sample_ids":  [jira_id],
            "description": description,
            "title":       title,
            "tags":        [jira_id.replace("-", "")],
            "dashboard":   "QC Dashboard",
            "quality":     0.75,
            "id":          jira_id
        }

        self.load_record(record, index="published_dashboards", id=jira_id)

    def index(self, index, doc_type, body):
        self.es.index(index=index, doc_type=doc_type, body=body)

    def delete_index(self, index):
        self.es.indices.delete(index=index, ignore=[400, 404])
