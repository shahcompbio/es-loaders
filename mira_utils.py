from mira.mira_metadata_parser import all_sample_ids, all_patient_sort_ids
from elasticsearch import Elasticsearch


def get_new_ids(type, host, port):
    if type == "sample":
        sample_ids = all_sample_ids()
        loaded_samples = get_loaded_ids("sample", host=host, port=port)

        return [sample_id for sample_id in sample_ids if sample_id not in loaded_samples]
    else:  # assume is patient
        patient_ids = all_patient_sort_ids()
        loaded_ids = get_loaded_ids("patient", host=host, port=port)

        return [patient_id for patient_id in patient_ids if patient_id not in loaded_ids]


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
