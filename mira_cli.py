import click
import logging
import logging.handlers
import os

from mira_loader import load_analysis as _load_analysis
from mira_cleaner import clean_analysis as _clean_analysis
from mira_data_checker import convert_metadata, check_analyses
from mira_utils import get_new_ids

LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"


@click.group()
@click.option('--host', default='localhost', help='Hostname for Elasticsearch server')
@click.option('--port', default=9200, help='Port for Elasticsearch server')
@click.option('--debug/--no-debug', default=False, help='Turn on debugging logs')
@click.pass_context
def main(ctx, host, port, debug):
    ctx.obj['host'] = host
    ctx.obj['port'] = port

    level = logging.DEBUG if debug else logging.INFO

    os.makedirs('logs/', exist_ok=True)
    handler = logging.handlers.TimedRotatingFileHandler(
        'logs/logfile.log', 'Midnight', 1)
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
def load_analysis(ctx, filepath, dashboard_id, type):
    try:
        _load_analysis(filepath, dashboard_id, type,
                       host=ctx.obj['host'], port=ctx.obj['port'])
    except:
        logger = ctx.obj['logger']
        logger.exception('Error while loading analysis: ' + dashboard_id)
        _clean_analysis(dashboard_id, type,
                        host=ctx.obj['host'], port=ctx.obj['port'])


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
