from mira_loader import load_analysis
from mira.mira_metadata_parser import patient_samples, all_samples

from elasticsearch import Elasticsearch

from common.scrna_parser import scRNAParser
from mira_cleaner import clean_analysis

import sys


def load_samples(filepath, patient_id, host="localhost", port=9200):

    samples = [sample_record["nick_unique_id"]
               for sample_record in patient_samples(patient_id)]
    for sample_id in samples:
        load_analysis(
            filepath, sample_id, "sample", host=host, port=port)


def load_new_samples(filepath, host="localhost", port=9200):
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
            except:
                e = sys.exc_info()
                print(e)
                clean_analysis("sample", sample_id, host=host, port=port)


def get_loaded_samples(host, port):
    es = Elasticsearch(hosts=[{'host': host, 'port': port}])

    result = es.search(index="dashboard_entry", body={"size": 1000})

    return [record["_source"]["dashboard_id"] for record in result["hits"]["hits"]]


def scrape_samples(filepath, patient_id):
    # samples = [sample_record["nick_unique_id"]
    #            for sample_record in patient_samples(patient_id)]
    samples = ["SPECTRUM-OV-022_S1_CD45P_RIGHT_ADNEXA", "SPECTRUM-OV-014_S2_CD45N_LEFT_ADNEXA",
               "SPECTRUM-OV-014_S2_CD45P_LEFT_ADNEXA",
               "SPECTRUM-OV-014_S2_CD45P_BOWEL", "SPECTRUM-OV-026_S1_CD45N_RIGHT_ADNEXA",
               "SPECTRUM-OV-026_S1_CD45P_LEFT_ADNEXA",
               "SPECTRUM-OV-026_S1_CD45N_BOWEL",
               "SPECTRUM-OV-026_S1_CD45P_RIGHT_ADNEXA",
               "SPECTRUM-OV-026_S1_CD45P_BOWEL",
               "SPECTRUM-OV-026_S1_CD45N_ASCITES",
               "SPECTRUM-OV-026_S1_CD45N_LEFT_ADNEXA"]
    print(samples)
    for sample_id in samples:
        print("===================== " + sample_id)
        data = scRNAParser(filepath + sample_id + ".rdata")
        print(data.data.colData.keys())


if __name__ == '__main__':
    load_new_samples(sys.argv[1],
                     host=sys.argv[2], port=sys.argv[3])
