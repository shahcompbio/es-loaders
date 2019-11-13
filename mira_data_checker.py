# Checks whether each dashboard has accidentally been uploaded multiple times, and deletes any duplicates

from elasticsearch import Elasticsearch
from mira_cleaner import clean_analysis
import sys
import click


def check_analyses(type, host, port):
    print("====================== CHECKING ANALYSIS")

    es = Elasticsearch(hosts=[{'host': host, 'port': port}])

    QUERY = {
        "size": 10000
    }
    result = es.search(index="sample_metadata", body=QUERY)

    all_ids = [record["_source"]["sample_id"]
               for record in result["hits"]["hits"]]

    unique_ids = set(all_ids)
    duplicate_ids = [
        dashboard_id for dashboard_id in unique_ids if all_ids.count(dashboard_id) > 1]

    for dashboard_id in duplicate_ids:
        print(dashboard_id)
        clean_analysis(type, dashboard_id, host=host, port=port)


if __name__ == "__main__":
    pass
