import sys
import math
import pandas as pd
from common.csv_loader import CSVLoader
from utils.elasticsearch import ElasticsearchClient
from utils.cli import CliClient


class GCMetricsLoader(CSVLoader):
    __INDEX_SUFFIX__ = "_gcmetrics"

    __METRICS_COLUMNS = ['cell_id', 'experimental_condition']

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

        [gc_metrics, metrics] = self._separate_tables(data)

        data = self._aggregate_gc(gc_metrics, metrics)

        super().transform_data(data)
        return data

    def _separate_tables(self, data_tables):
        return data_tables

    def _aggregate_gc(self, gc_metrics, metrics):
        sub_metrics = metrics[self.__METRICS_COLUMNS]
        merged_table = pd.merge(gc_metrics, sub_metrics, how="inner")

        experimental_conditions = merged_table['experimental_condition'].unique(
        )

        dfs = []

        for exp_con in experimental_conditions:
            # Get rows with just exp_con
            sub_table = merged_table.loc[merged_table['experimental_condition']
                                         == exp_con].iloc[0:, :101]
            ranked_table = sub_table.apply(lambda x: x.sort_values().values)

            # Calculate median + confidence intervals
            N = len(sub_table.index)

            low_ci_index = max(0,
                               abs(math.ceil((N / 2) - (1.96 * math.sqrt(N) / 2))))
            high_ci_index = min(N-1, math.floor(
                1 + (N / 2) + (1.96 * math.sqrt(N) / 2)) - 1)

            ci_table = ranked_table.iloc[[
                low_ci_index, high_ci_index]].transpose()
            median_table = sub_table.median()

            # construct data table
            joined_table = pd.concat([ci_table, median_table], axis=1)
            joined_table.columns = ['low_ci', 'high_ci', 'median']

            joined_table['gc_percent'] = joined_table.index.map(
                lambda x: int(x))
            joined_table['experimental_condition'] = exp_con

            dfs = dfs + [joined_table]

        data = pd.concat(dfs)

        return data


def main():
    CLI = CliClient('GC Metrics Loader')
    CLI.add_loader_argument(isFilepath=True)
    CLI.add_elasticsearch_arguments()

    print("STARTING GC METRICS LOAD")
    args = CLI.get_args()
    loader = GCMetricsLoader()
    loader.load_data(args.index_name, args.filepaths,
                     host=args.es_host, port=args.es_port)


if __name__ == '__main__':
    main()
