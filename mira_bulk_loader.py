from mira_loader import load_analysis
from mira.mira_metadata_parser import patient_samples

from common.scrna_parser import scRNAParser

import sys

BROKEN_SAMPLE_IDS = ["SPECTRUM-OV-003_S1_CD45N_RIGHT_ADNEXA",
                     "SPECTRUM-OV-003_S2_CD45N_RIGHT_ADNEXA",
                     "SPECTRUM-OV-026_S1_CD45N_BOWEL",
                     "SPECTRUM-OV-031_S1_CD45N_LYMPH_NODE",
                     "SPECTRUM-OV-036_S1_CD45P_RIGHT_ADNEXA",
                     "SPECTRUM-OV-003_S2_CD45P_PERITONEUM",
                     "SPECTRUM-OV-009_S1_CD45P_RIGHT_UPPER_QUADRANT",
                     "SPECTRUM-OV-022_S1_CD45N_RIGHT_ADNEXA",
                     "SPECTRUM-OV-007_S1_CD45N_BOWEL",
                     "SPECTRUM-OV-026_S1_CD45P_ASCITES"]


def load_samples(filepath, patient_id, host="localhost", port=9200):

    samples = [sample_record["nick_unique_id"]
               for sample_record in patient_samples(patient_id)]
    print
    for sample_id in samples:
        if sample_id not in BROKEN_SAMPLE_IDS:
            load_analysis(
                filepath, sample_id, "sample", host=host, port=port)


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
    load_samples(sys.argv[1], sys.argv[4], host=sys.argv[2], port=sys.argv[3])
