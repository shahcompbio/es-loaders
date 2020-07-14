import click
import logging
import logging.handlers
import os

from mira.mira_loader import load_analysis as _load_analysis, load_celltype_data, load_dashboard_entry as _load_dashboard_entry
from mira.mira_isabl import get_new_isabl_analyses
from mira.mira_data import download_analyses_data, get_celltype_analyses, download_cohort_data, download_metadata
from mira.elasticsearch import clean_analysis as _clean_analysis, load_rho as _load_rho, clean_rho as _clean_rho, clean_dashboard_entry, clean_genes as _clean_genes
from mira.rho_loader import download_rho_data, generate_dashboard_rho
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
@click.option('--chunksize', help="How many milions of records to chunk matrix file", type=int)
@click.option('--download',  is_flag=True,help="Download file if missing", type=int)
@click.option('--load-new', is_flag=True, help="Load dashboards not currently in Mira")
def load_analysis(ctx, data_directory, id, type, reload, chunksize, download, load_new):
    assert id is not None or load_new

    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    if load_new:
        analyses_metadata = get_new_isabl_analyses(type, load_new=True, es_host=es_host, es_port=es_port)
    else:
        analyses_metadata = get_new_isabl_analyses(type, dashboard_id=id, es_host=es_host, es_port=es_port)
    analyses = [{**analysis, "directory": data_directory if data_directory.endswith(analysis["dashboard_id"]) else os.path.join(data_directory, analysis["dashboard_id"]) } for analysis in analyses_metadata]

    if download:
        download_analyses_data(analyses, data_directory)

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
@click.option('--reload', is_flag=True, help="Force reload this library")
@click.option('--chunksize', help="How many milions of records to chunk matrix file", type=int)
@click.option('--download',  is_flag=True,help="Download file if missing", type=int)
def load_cohort(ctx, data_directory, reload, chunksize, download):

    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    cohort_analysis = get_new_isabl_analyses("cohort", es_host=es_host, es_port=es_port)
    cohort_celltype_analyses = get_celltype_analyses(cohort_analysis)

    if download:
        download_cohort_data(cohort_analysis, cohort_celltype_analyses, data_directory)

    metadata = {
        "date": cohort_analysis["modified"]
    }
    
    # if reload:
    #     _clean_analysis(cohort_analysis["dashboard_id"], host=es_host, port=es_port)

    # _load_analysis(os.path.join(data_directory), cohort_analysis["dashboard_id"], es_host, es_port, isCohort=True, chunksize=chunksize * int(1e6), metadata=metadata)

    for analysis in cohort_celltype_analyses:
        if reload:
            _clean_analysis(analysis["dashboard_id"], host=es_host, port=es_port)

        load_celltype_data(data_directory, analysis["dashboard_id"], es_host, es_port, chunksize=chunksize * int(1e6), metadata=metadata)

@main.command()
@click.argument('dashboard_id')
@click.option('--reload', is_flag=True, help="Force reload")
@click.pass_context
def load_rho(ctx, dashboard_id, reload):
    host = ctx.obj['host']
    port = ctx.obj['port']
    if reload:
        _clean_rho(host, port)

    data = download_rho_data()
    data = generate_dashboard_rho(data, dashboard_id)
    _load_rho(data, host=host, port=port)


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
@click.argument('data_directory')
@click.pass_context
@click.option('--type', required=True, type=click.Choice(['patient','cohort'], case_sensitive=False), help="Type of dashboard")
@click.option('--id', help="ID of dashboard")
@click.option('--reload', is_flag=True, help="Force reload this library")
def generate_sample_metadata(ctx, data_directory, type, id, reload):

    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    analyses_metadata = get_new_isabl_analyses(type, es_host=es_host, es_port=es_port)

    download_metadata(analyses_metadata, data_directory)

@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--type', required=True, type=click.Choice(['patient','cohort'], case_sensitive=False), help="Type of dashboard")
@click.option('--id', help="ID of dashboard")
@click.option('--reload', is_flag=True, help="Force reload this library")
def generate_sample_metadata_cohort(ctx, data_directory, type, id, reload):

    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    analyses_metadata = get_new_isabl_analyses(type, es_host=es_host, es_port=es_port)

    download_metadata(analyses_metadata, data_directory)


@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--type', required=True, type=click.Choice(['patient','cohort'], case_sensitive=False), help="Type of dashboard")
@click.option('--id', help="ID of dashboard")
@click.option('--reload', is_flag=True, help="Force reload this library")
def load_dashboard_entry(ctx, data_directory, type, id, reload):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    analyses_metadata = get_new_isabl_analyses(type, es_host=es_host, es_port=es_port)

    analyses = [{**analysis, "directory": data_directory if data_directory.endswith(analysis["dashboard_id"]) or type == "cohort" else os.path.join(data_directory, analysis["dashboard_id"]) } for analysis in analyses_metadata]


    for analysis in analyses:
        metadata = {
            "date": analysis["modified"]
        }

        if reload:
            clean_dashboard_entry(analysis["dashboard_id"], es_host, es_port)

        _load_dashboard_entry(analysis["directory"], type, analysis["dashboard_id"], metadata, es_host, es_port)
    


@main.command()
@click.argument('data_directory')
@click.pass_context
@click.option('--id', help="ID of dashboard")
@click.option('--reload', is_flag=True, help="Force reload this library")
@click.option('--download', is_flag=True, help="Download files")
def load_dashboard_entry_cohort(ctx, data_directory, id, reload, download):
    es_host = ctx.obj['host']
    es_port = ctx.obj["port"]

    cohort_analysis = get_new_isabl_analyses("cohort", es_host=es_host, es_port=es_port)
    cohort_celltype_analyses = get_celltype_analyses(cohort_analysis)

    if download:
        download_cohort_data(cohort_analysis, cohort_celltype_analyses, data_directory)

    metadata = {
        "date": cohort_analysis["modified"]
    }

    for analysis in cohort_celltype_analyses:
        if reload:
            clean_dashboard_entry(analysis["dashboard_id"], es_host, es_port)

        _load_dashboard_entry(data_directory, "cohort", analysis["dashboard_id"], metadata, es_host, es_port)

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
