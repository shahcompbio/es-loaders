from mira_loader import load_analysis
from mira.mira_metadata_parser import patient_samples, all_samples

from elasticsearch import Elasticsearch

from common.scrna_parser import scRNAParser
from mira_cleaner import clean_analysis

import click
import sys


@click.command()
@click.argument('filepath')
@click.option('--host', default='localhost', help='Hostname for Elasticsearch server')
@click.option('--port', default=9200, help='Port for Elasticsearch server')
def load_new_samples(filepath, host, port):
    all_sample_ids = [sample_id
                      for sample_id, fields in all_samples().items()]

    loaded_samples = get_loaded_samples(host=host, port=port)

    for sample_id in all_sample_ids:
        if sample_id in loaded_samples:
            pass
        else:
            try:
                load_analysis(
                    filepath, sample_id, "sample", host=host, port=port)
            except KeyboardInterrupt as e:
                print(e)
                clean_analysis("sample", sample_id, host=host, port=port)
                break
            except:
                e = sys.exc_info()
                print(e)
                clean_analysis("sample", sample_id, host=host, port=port)


def get_loaded_samples(host, port):
    es = Elasticsearch(hosts=[{'host': host, 'port': port}])

    result = es.search(index="dashboard_entry", body={"size": 1000})

    return [record["_source"]["dashboard_id"] for record in result["hits"]["hits"]]


if __name__ == '__main__':
    load_new_samples()
