from elasticsearch import Elasticsearch
import sys

SAMPLE_METADATA_INDEX = "sample_metadata"
SAMPLE_STATS_INDEX = "sample_stats"
SAMPLE_CELLS_INDEX = "sample_cells"
DASHBOARD_REDIM_INDEX = "dashboard_redim_"
DASHBOARD_GENES_INDEX = "dashboard_genes_"
DASHBOARD_ENTRY_INDEX = "dashboard_entry"


def clean_analysis(type, dashboard_id, host="localhost", port=9200):
    print("====================== " + dashboard_id)
    print("Cleaning records")

    if type is "sample":
        print("DELETE SAMPLE METADATA")
        delete_records(SAMPLE_METADATA_INDEX, "sample_id",
                       dashboard_id, host=host, port=port)

        print("DELETE SAMPLE CELLS")
        delete_records(SAMPLE_CELLS_INDEX, "sample_id",
                       dashboard_id, host=host, port=port)

        print("DELETE SAMPLE STATS")
        delete_records(SAMPLE_STATS_INDEX, "sample_id",
                       dashboard_id, host=host, port=port)

    print("DELETE DASHBOARD REDIM")
    delete_index(DASHBOARD_REDIM_INDEX +
                 dashboard_id.lower(), host=host, port=port)

    print("DELETE DASHBOARD GENES")
    delete_index(DASHBOARD_GENES_INDEX +
                 dashboard_id.lower(), host=host, port=port)

    print("DELETE DASHBOARD_ENTRY")
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
    clean_analysis(sys.argv[1], sys.argv[2])
