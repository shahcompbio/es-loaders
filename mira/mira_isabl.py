import isabl_cli as ii
from mira.elasticsearch import is_dashboard_loaded
import os

import logging
logger = logging.getLogger('mira_loading')



APP_VERSION = '1.0.0'
os.environ["ISABL_API_URL"] = 'https://isabl.shahlab.mskcc.org/api/v1/'
os.environ['ISABL_CLIENT_ID'] = '1'


def get_new_isabl_analyses(type, dashboard_id=None, load_new=False, es_host='localhost', es_port=9200):

    logger.info("===================== Fetching Isabl scRNA analyses")
    analyses = get_isabl_scrna_analyses(type)

    logger.debug(f'Analyses from Isabl: {len(analyses)}')

    if load_new:
        analyses = [analysis for analysis in analyses if not is_dashboard_loaded(analysis["dashboard_id"], analysis["modified"], es_host, es_port)]
        logger.debug(f'Analyses after filtering through Mira: {len(analyses)}')

    if dashboard_id is not None:
        logger.debug(f'Patient ID is not None')
        analyses = [analysis for analysis in analyses if analysis["dashboard_id"] == dashboard_id]

    logger.info(f'Returning {len(analyses)} analyses')
    return analyses


def get_isabl_scrna_analyses(type):
    latest_scrna_rdata = []
    all_analyses = ii.get_instances('analyses', application__name__in='CELLASSIGN Individual Application, CELLASSIGN Project Application')

    if type == "cohort":
        cohort_analyses = [analysis for analysis in all_analyses if is_cohort_analysis(analysis)]

        cohort_analyses = [{
            **_process_analysis(analysis),
            "dashboard_id": "cohort_all"
        } for analysis in cohort_analyses]

        return cohort_analyses

    else: # is patient
        analyses = [{**_process_analysis(analysis), "dashboard_id": analysis["individual_level_analysis"]["identifier"]} for analysis in all_analyses if is_patient_analysis(analysis)]

    return analyses


def _process_analysis(analysis):
    return {
        "pk": analysis["pk"],
        "modified": analysis["modified"],
        "juno_storage": analysis["storage_url"]
    }

def is_patient_analysis(analysis):
    return analysis['application']['name'] == 'CELLASSIGN Individual Application' \
         and analysis['application']['version'] == APP_VERSION \
         and analysis['status'] == 'SUCCEEDED'  

def is_cohort_analysis(analysis):
    return analysis['application']['name'] == 'CELLASSIGN Project Application' \
         and analysis['status'] == 'SUCCEEDED'  





# analyses = get_new_isabl_analyses(load_new=True, es_host='plvicosspecdat2', es_port=9200)
# print(analyses)