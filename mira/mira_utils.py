from elasticsearch import Elasticsearch

from mira.metadata_parser import MiraMetadata


def get_new_ids(type, host, port, metadata):
    all_ids = metadata.sample_ids() if type == "sample" else metadata.patient_sort_ids()
    loaded_ids = get_loaded_ids(type, host=host, port=port)

    return [dashboard_id for dashboard_id in all_ids if dashboard_id not in loaded_ids]


def get_loaded_ids(type, host, port):
    es = Elasticsearch(hosts=[{'host': host, 'port': port}])

    result = es.search(index="dashboard_entry", body={
        "size": 1000,
        "query": {
            "bool": {
                "filter": {
                    "term": {
                        "type": type
                    }
                }
            }
        }
    })

    return [record["_source"]["dashboard_id"] for record in result["hits"]["hits"]]
