from mira_loader import load_analysis
from mira.mira_metadata_parser import all_samples

import sys


def load_samples(filepath, host="localhost", port=9200):

    samples = all_samples()
    for sample_id, sample_record in samples.items():
        load_analysis(
            filepath, sample_id, "sample", host=host, port=port)


if __name__ == '__main__':
    load_samples(sys.argv[1], host=sys.argv[2], port=sys.argv[3])
