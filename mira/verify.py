
from elasticsearch import Elasticsearch
import logging

from mira.metadata_parser import MiraMetadata

logger = logging.getLogger('mira_loading')


def verify_indices(host='localhost', port=9200):
    es = Elasticsearch(
        hosts=[{'host': host, 'port': port}])

    loaded_dashboards = get_loaded_dashboards(es)
    dashboard_ids = [dashboard["dashboard_id"]
                     for dashboard in loaded_dashboards]
    unique_ids = set(dashboard_ids)

    # check for duplicates
    duplicate_ids = [
        dashboard_id for dashboard_id in unique_ids if dashboard_ids.count(dashboard_id) > 1]

    logger.info("=========== DUPLICATE IDS: " + str(len(duplicate_ids)))
    for dashboard_id in duplicate_ids:
        logger.info(dashboard_id)

    # Check whether all patient dashboards have supporting samples
    patient_dashboard_ids = [dashboard["dashboard_id"]
                             for dashboard in loaded_dashboards if not dashboard["type"] == "sample"]

    metadata = MiraMetadata()
    for dashboard_id in patient_dashboard_ids:
        support_sample_ids = metadata.support_sample_ids(dashboard_id)

        missing_samples = [
            sample_id for sample_id in support_sample_ids if not sample_id in dashboard_ids]

        if len(missing_samples) > 0:
            logger.info("======= MISSING " + str(len(missing_samples)) +
                        " SAMPLES FOR " + dashboard_id)
            for sample_id in missing_samples:
                logger.info(sample_id)


def missing_dashboards(host='localhost', port=9200):

    es = Elasticsearch(
        hosts=[{'host': host, 'port': port}])
    loaded_dashboards = [record["dashboard_id"]
                         for record in get_loaded_dashboards(es)]

    metadata = MiraMetadata()
    sample_ids = metadata.sample_ids()

    missing_sample_ids = [
        sample_id for sample_id in sample_ids if not sample_id in loaded_dashboards]

    logger.info("====== MISSING SAMPLE DASHBOARDS: " +
                str(len(missing_sample_ids)))
    for missing_sample_id in missing_sample_ids:
        logger.info(missing_sample_id)

    patient_ids = metadata.patient_ids()
    missing_patient_ids = [
        patient_id for patient_id in patient_ids if not patient_id in loaded_dashboards]

    logger.info("====== MISSING PATIENT DASHBOARDS: " +
                str(len(missing_patient_ids)))
    for missing_patient_id in missing_patient_ids:
        logger.info(missing_patient_id)


def get_loaded_dashboards(es):
    QUERY = {
        "size": 10000
    }

    result = es.search(index="dashboard_entry", body=QUERY)

    return [record["_source"] for record in result["hits"]["hits"]]
