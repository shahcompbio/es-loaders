import click
import logging
import logging.handlers
import os

from mira.mira_loader import load_analysis as _load_analysis
from mira.mira_cleaner import clean_analysis as _clean_analysis, delete_index
from mira.mira_utils import get_new_ids
from mira.rho_loader import load_rho as _load_rho
from mira.metadata_parser import MiraMetadata
from mira.verify import verify_indices


from elasticsearch import Elasticsearch

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"


@click.group()
@click.option('--host', default='localhost', help='Hostname for Elasticsearch server')
@click.option('--port', default=9200, help='Port for Elasticsearch server')
@click.option('--debug', is_flag=True, help='Turn on debugging logs')
@click.pass_context
def main(ctx, host, port, debug):
    ctx.obj['host'] = host
    ctx.obj['port'] = port

    level = logging.DEBUG if debug else logging.INFO

    os.makedirs('logs/', exist_ok=True)
    handler = logging.handlers.TimedRotatingFileHandler(
        'logs/logfile.log', 'midnight', 1)
    handler.suffix = "%Y-%m-%d"

    logging.basicConfig(format=LOGGING_FORMAT, handlers=[
                        handler, logging.StreamHandler()])

    logger = logging.getLogger('mira_loading')
    logger.setLevel(level)

    ctx.obj['logger'] = logger


@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--type', required=True, type=click.Choice(['sample', 'patient'], case_sensitive=False), help="Type of dashboard")
@click.option('--id', help="ID of dashboard")
@click.option('--reload', is_flag=True, help="Force reload this library")
@click.option('--load-support', is_flag=True, help="Load supporting samples")
@click.option('--load-new', is_flag=True, help="Load dashboards not currently in Mira")
def load_analysis(ctx, data_directory, id, type, reload, load_support, load_new):
    # one of ID or load_new must be present
    assert id is not None or load_new is not None, "Must specify one of ID or load_new"

    metadata = MiraMetadata()

    to_load = [[id, type]] if id is not None else [[new_id, type]
                                                   for new_id in get_new_ids(type, ctx.obj['host'], ctx.obj['port'], metadata)]

    if load_support:
        assert not type == 'sample', 'Cannot load supporting samples for sample type'

        to_load_new = []
        for patient_dashboard in to_load:
            to_load_new = to_load_new + [[sample_id, "sample"]
                                         for sample_id in metadata.support_sample_ids(patient_dashboard[0])] + [patient_dashboard]

        to_load = to_load_new

    load_analysis_list(data_directory, to_load, ctx.obj['logger'], ctx.obj['host'],
                       ctx.obj['port'], metadata=metadata, reload=reload)


@main.command()
@click.argument('filepath')
@click.pass_context
def update_to_v2(ctx, filepath):
    es = Elasticsearch(
        hosts=[{'host': ctx.obj['host'], 'port':ctx.obj['port']}])

    new_entries = []
    if es.indices.exists("dashboard_cells"):
        NEW_QUERY = {
            "size": 0,
            "aggs": {
                "agg_terms_dashboard_id": {
                    "terms": {
                        "field": "dashboard_id",
                        "size": 1000,
                        "order": {
                            "_key": "asc"
                        }
                    }
                }
            }
        }

        new_result = es.search(index="dashboard_cells", body=NEW_QUERY)

        new_entries = [record["key"] for record in new_result["aggregations"]
                       ["agg_terms_dashboard_id"]["buckets"]]

    QUERY = {
        "size": 10000
    }
    result = es.search(index="dashboard_entry", body=QUERY)

    to_load = [record["_source"]
               for record in result["hits"]["hits"] if record["_source"]["dashboard_id"] not in new_entries]
    load_analysis_list(filepath, to_load, ctx.obj['logger'], ctx.obj['host'],
                       ctx.obj['port'], reload=True)

    verify_indices(host=ctx.obj['host'], port=ctx.obj['port'])


def load_analysis_list(filepath, to_load, logger, host, port, reload=False, metadata=None):

    if metadata is None:
        metadata = MiraMetadata()

    for load_record in to_load:
        load_id = load_record[0]
        load_type = load_record[1]
        if reload:
            _clean_analysis(load_id, load_type,
                            host=host, port=port)
        try:
            if is_loaded(
                    load_id, load_type, host=host, port=port):
                logger.warning(
                    "===== DASHBOARD has been loaded before - will skip: " + load_id)

            else:
                _load_analysis(filepath, load_id, load_type, metadata=metadata,
                               host=host, port=port)

        except KeyboardInterrupt:
            logger.exception()
            _clean_analysis(load_id, load_type,
                            host=host, port=port)
            break

        except:
            logger.exception('Error while loading analysis: ' + load_id)
            _clean_analysis(load_id, load_type,
                            host=host, port=port)
            continue


def is_loaded(dashboard_id, type, host, port):
    es = Elasticsearch(
        hosts=[{'host': host, 'port': port}])
    QUERY = {
        "query": {
            "bool": {
                "filter": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "dashboard_id": dashboard_id
                                }
                            },
                            {
                                "term": {
                                    "type": type
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
    result = es.search(index="dashboard_entry", body=QUERY)

    return result["hits"]["total"]["value"] > 0


@main.command()
@click.pass_context
def load_rho(ctx):
    host = ctx.obj['host']
    port = ctx.obj['port']
    _load_rho(host, port)


@main.command()
@click.pass_context
def verify_load(ctx):
    host = ctx.obj['host']
    port = ctx.obj['port']
    verify_indices(host=host, port=port)


@main.command()
@click.argument('dashboard_id')
@click.argument('type')
@click.pass_context
def clean_analysis(ctx, dashboard_id, type):
    _clean_analysis(dashboard_id, type,
                    host=ctx.obj['host'], port=ctx.obj['port'])


def start():
    main(obj={})


if __name__ == '__main__':
    start()
