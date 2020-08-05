import click
import logging
import logging.handlers
import os

from mira.mira_loader import load_analysis as _load_analysis, load_celltype_data, load_dashboard_entry as _load_dashboard_entry
from mira.mira_isabl import get_new_isabl_analyses
from mira.mira_data import download_analyses_data, get_celltype_analyses, download_metadata
from mira.elasticsearch import clean_analysis as _clean_analysis, load_rho as _load_rho, clean_rho as _clean_rho, clean_dashboard_entry, clean_genes as _clean_genes
from mira.gene_loader import load_gene_names as _load_genes


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
@click.option('--type', required=True, type=click.Choice(['patient','cohort'], case_sensitive=False), help="Type of dashboard")
@click.option('--id', help="ID of dashboard")
@click.option('--reload', is_flag=True, help="Force reload this library")
@click.option('--chunksize', help="How many milions of records to chunk matrix file", type=int, default=1)
@click.option('--download',  is_flag=True,help="Download file if missing", type=int)
@click.option('--load-new', is_flag=True, help="Load dashboards not currently in Mira")
@click.option('--load-cohort', type=click.Choice(['cohort','cell_type', 'both']), help="Load cohort, cell types, or both")
def load_analyses(ctx, data_directory, type,id,  reload, chunksize, download, load_new, load_cohort):
    assert id is not None or load_new

    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    if load_new:
        analyses_metadata = get_new_isabl_analyses(type, load_new=True, es_host=es_host, es_port=es_port)
    else:
        analyses_metadata = get_new_isabl_analyses(type, dashboard_id=id, es_host=es_host, es_port=es_port)

    if type == "cohort":
        ## We will make the naive assumption that there will only ever be one cohort analysis entry in Isabl for the entire project (LOL)
        if load_cohort == "cohort":
            # do nothing
            pass
        elif load_cohort == "cell_type":
            cohort_celltype_analyses = get_celltype_analyses(analyses_metadata[0])
            analyses_metadata = cohort_celltype_analyses
        elif load_cohort == "both":
            cohort_celltype_analyses = get_celltype_analyses(analyses_metadata[0])
            analyses_metadata = analyses_metadata + cohort_celltype_analyses

    if download:
        download_analyses_data(type, analyses_metadata, data_directory, cohort_group=load_cohort)

    analyses = [{**analysis, "directory": data_directory if data_directory.endswith(analysis["dashboard_id"]) else os.path.join(data_directory, analysis["dashboard_id"]) } for analysis in analyses_metadata]

    for analysis in analyses:
        metadata = {
            "date": analysis["modified"]
        }

        if reload:
            _clean_analysis(analysis["dashboard_id"], host=es_host, port=es_port)

        _load_analysis(analysis["directory"], type, analysis["dashboard_id"], es_host, es_port, chunksize=chunksize * int(1e6), metadata=metadata)



@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--type', case_sensitive=False), help="Type of dashboard")
@click.option('--id', help="ID of dashboard")
@click.option('--reload', is_flag=True, help="Force reload this library")
@click.option('--chunksize', help="How many milions of records to chunk matrix file", type=int, default=1)
def load_analysis(ctx, data_directory, type,id,  reload, chunksize):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    if reload:
        _clean_analysis(analysis["dashboard_id"], host=es_host, port=es_port)

    _load_analysis(analysis["directory"], type, analysis["dashboard_id"], es_host, es_port, chunksize=chunksize * int(1e6))


@main.command()
@click.argument('directory')
@click.option('--reload', is_flag=True, help="Force reload")
@click.pass_context
def load_genes(ctx, directory, reload):
    host = ctx.obj['host']
    port = ctx.obj['port']
    if reload:
        _clean_genes(host, port)

    _load_genes(directory, host=host, port=port)



@main.command()
@click.argument('dashboard_id')
@click.pass_context
def clean_analysis(ctx, dashboard_id):
    _clean_analysis(dashboard_id,
                    host=ctx.obj['host'], port=ctx.obj['port'])


def start():
    main(obj={})


if __name__ == '__main__':
    start()
