import sys
from common.metrics_loader import MetricsLoader
from common.copy_number_loader import CopyNumberLoader
from common.gc_metrics_loader import GCMetricsLoader
from analysis_loader import AnalysisLoader


from utils.cli import CliClient


class AlhenaLoader():

    __ALIGNMENT_FILENAME = "alignment_metrics.csv.gz"
    __METRICS_FILENAME = "metrics.csv.gz"
    __SEGMENTS_FILENAME = "segments.csv.gz"
    __READS_FILENAME = "reads.csv.gz"
    __GC_METRICS_FILENAME = "gc_metrics.csv.gz"

    def __init__(self):
        pass

    def load_all(self, root_path, jira_id, host, port, colossus_user, colossus_pw):

        def load_data_type(data_type, index_name, loader, files):
            self.print_header(jira_id + ": " + data_type.upper())

            nonlocal root_path
            if not root_path.endswith("/"):
                root_path = root_path + "/"

            if isinstance(files, list):
                file_paths = map(lambda file: root_path + file, files)
            else:
                file_paths = root_path + files

            loader.load_data(index_name, file_paths, host=host, port=port)

        self.print_header("STARTING LOADING FOR " + jira_id)

        load_data_type("metrics", jira_id, MetricsLoader(), [
            self.__ALIGNMENT_FILENAME, self.__METRICS_FILENAME])

        load_data_type("gc metrics", jira_id, GCMetricsLoader(), [
            self.__GC_METRICS_FILENAME, self.__METRICS_FILENAME])

        load_data_type("segments", jira_id + "_segs", CopyNumberLoader(),
                       self.__SEGMENTS_FILENAME)

        load_data_type("reads", jira_id + "_reads",
                       CopyNumberLoader(), self.__READS_FILENAME)

        self._load_analysis_entry(jira_id, user=colossus_user,
                                  password=colossus_pw, host=host, port=port)

    def _load_analysis_entry(self, jira_id, user, password, host, port):
        self.print_header("Analysis Entry")
        loader = AnalysisLoader()
        loader.load_data(jira_id, user=user,
                         password=password, host=host, port=port)

    def print_header(self, text):
        print(" ======================== " +
              text + " ======================== ")


def main():
    CLI = CliClient('Alhena Loader')
    CLI.add_loader_argument(isFilepath=False)
    CLI.add_elasticsearch_arguments()
    CLI.add_colossus_arguments()

    print("STARTING ALHENA LOAD")
    args = CLI.get_args()
    loader = AlhenaLoader()
    loader.load_all(args.file_root, args.index_name,
                    host=args.es_host, port=args.es_port,
                    colossus_user=args.colossus_user, colossus_pw=args.colossus_pass)


if __name__ == '__main__':
    main()
