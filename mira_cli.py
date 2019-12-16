import click
import logging
import logging.handlers
import os

from mira_loader import load_analysis as _load_analysis
from mira_cleaner import clean_analysis as _clean_analysis
from mira_data_checker import convert_metadata, check_analyses
from mira_utils import get_new_ids
from rho_loader import load_rho as _load_rho


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
@click.argument('filepath')
@click.argument('dashboard_id')
@click.argument('type')
@click.pass_context
@click.option('--reload', is_flag=True, help="Force reload this library")
def load_analysis(ctx, filepath, dashboard_id, type, reload):
    if reload:
        _clean_analysis(dashboard_id, type,
                        host=ctx.obj['host'], port=ctx.obj['port'])
    try:
        assert _is_not_loaded(
            dashboard_id, type, ctx.obj['host'], ctx.obj['port']), dashboard_id + " has already been loaded"

        _load_analysis(filepath, dashboard_id, type,
                       host=ctx.obj['host'], port=ctx.obj['port'])
    except:
        logger = ctx.obj['logger']
        logger.exception('Error while loading analysis: ' + dashboard_id)
        _clean_analysis(dashboard_id, type,
                        host=ctx.obj['host'], port=ctx.obj['port'])


def _is_not_loaded(dashboard_id, type, host, port):
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
                                    "term": term
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
    result = es.search(index="dashboard_entry", body=QUERY)

    return result["hits"]["total"]["value"] == 0


@main.command()
@click.argument('filepath')
@click.pass_context
def reload_all_analysis(ctx, filepath):
    es = Elasticsearch(
        hosts=[{'host': ctx.obj['host'], 'port':ctx.obj['port']}])
    QUERY = {
        "size": 10000
    }
    result = es.search(index="dashboard_entry", body=QUERY)

    all_entries = [record["_source"]
                   for record in result["hits"]["hits"]]

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

    logger = ctx.obj['logger']
    for record in all_entries:
        dashboard_id = record["dashboard_id"]
        logger.info(dashboard_id)
        if dashboard_id in new_entries:
            logger.info("=== RELOADING: " + dashboard_id)
            _clean_analysis(dashboard_id, record["type"],
                            host=ctx.obj['host'], port=ctx.obj['port'])

            try:
                _load_analysis(filepath, dashboard_id, record["type"],
                               host=ctx.obj['host'], port=ctx.obj['port'])

            except KeyboardInterrupt as err:
                logger.exception()
                _clean_analysis(dashboard_id, record["type"],
                                host=ctx.obj['host'], port=ctx.obj['port'])
                break

            except:
                logger.exception(
                    'Error while loading analysis: ' + dashboard_id)
                _clean_analysis(dashboard_id, record["type"],
                                host=ctx.obj['host'], port=ctx.obj['port'])
                continue


@main.command()
@click.argument('dir')
@click.argument('type')
@click.pass_context
def load_new_ids_by_type(ctx, dir, type):
    host = ctx.obj['host']
    port = ctx.obj['port']

    dashboard_ids = get_new_ids(type, host=host, port=port)

    for dashboard_id in dashboard_ids:
        try:
            # file path is also dependent on type?
            _load_analysis(
                dir, dashboard_id, type, host=host, port=port)
        except KeyboardInterrupt as err:
            logger = ctx.obj['logger']
            logger.exception()
            _clean_analysis(dashboard_id, type, host=host, port=port)
            break
        except Exception as err:
            logger = ctx.obj['logger']
            logger.exception('Error while loading analysis: ' + dashboard_id)
            _clean_analysis(dashboard_id, type,
                            host=ctx.obj['host'], port=ctx.obj['port'])
            continue


@main.command()
@click.pass_context
def load_rho(ctx):
    host = ctx.obj['host']
    port = ctx.obj['port']
    _load_rho(host, port)


@main.command()
@click.pass_context
def reload_metadata(ctx):
    host = ctx.obj['host']
    port = ctx.obj['port']

    convert_metadata(host, port)


@main.command()
@click.argument('dashboard_id')
@click.argument('type')
@click.pass_context
def clean_analysis(ctx, dashboard_id, type):
    _clean_analysis(dashboard_id, type,
                    host=ctx.obj['host'], port=ctx.obj['port'])


@main.command()
@click.argument('type')
@click.pass_context
def clean_duplicate_analyses(ctx, type):
    check_analyses(type, host=ctx.obj['host'], port=ctx.obj['port'])


@main.command()
@click.pass_context
def test(ctx):
    logger = ctx.obj['logger']
    logger.info("huzzah!")


def start():
    main(obj={})


if __name__ == '__main__':
    start()
