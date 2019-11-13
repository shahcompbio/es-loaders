from mira.mira_metadata_parser import patient_samples, all_samples
from elasticsearch import Elasticsearch


def get_new_sample_ids(host, port):
    all_sample_ids = [sample_id
                      for sample_id, fields in all_samples().items()]

    loaded_samples = get_loaded_samples(host=host, port=port)

    return [sample_id for sample_id in all_sample_ids if sample_id not in loaded_samples]


def get_loaded_samples(host, port):
    es = Elasticsearch(hosts=[{'host': host, 'port': port}])

    result = es.search(index="dashboard_entry", body={"size": 1000})

    return [record["_source"]["dashboard_id"] for record in result["hits"]["hits"]]
