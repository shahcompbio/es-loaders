import sys
import pandas as pd
from common.csv_loader import CSVLoader
from utils.elasticsearch import ElasticsearchClient
from utils.cli import CliClient


class MetricsLoader(CSVLoader):

    __INDEX_SUFFIX__ = "_metrics"

    def __init__(self):
        super().__init__()

    def load_data(self, index_name, filepaths, host, port):
        print("OPENING FILES")

        data_tables = list(map(
            lambda filepath: self.extract_file(filepath), filepaths))
        print("TRANSFORM DATA")
        data = self.transform_data(data_tables)

        print("LOAD INTO ES")
        self.load_to_es(index_name + self.__INDEX_SUFFIX__,
                        data, host=host, port=port)

    def transform_data(self, data):
        data = pd.merge(data[0], data[1], how="inner")
        super().transform_data(data)
        return data


def main():
    CLI = CliClient('Metrics Loader')
    CLI.add_loader_argument(isFilepath=True)
    CLI.add_elasticsearch_arguments()

    print("STARTING QC METRICS LOAD")
    args = CLI.get_args()
    loader = MetricsLoader()
    loader.load_data(args.index_name, args.file_paths,
                     host=args.es_host, port=args.es_port)


if __name__ == '__main__':
    main()
