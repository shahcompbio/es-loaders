import sys
from utils.colossus import ColossusClient
from utils.elasticsearch import ElasticsearchClient

from utils.cli import CliClient


class AnalysisLoader():

    __INDEX_NAME__ = "analyses"

    def __init__(self):
        pass

    def load_data(self, jira_id, user, password, host, port):

        es = ElasticsearchClient(host=host, port=port)
        colossus = ColossusClient()

        analysis_record = colossus.get_analysis_information(
            jira_id, user, password)

        record = self._get_record(analysis_record)

        es.load_record(self.__INDEX_NAME__, record)

    def load_all_analyses(self, user, password, host, port):
        colossus = ColossusClient()

        colossus_records = colossus.get_all_analyses_information(
            user, password)
        analysis_generator = self._get_analysis_generator(colossus_records)

        es = ElasticsearchClient(host=host, port=port)
        es.load_in_bulk(self.__INDEX_NAME__, analysis_generator)

    def _get_analysis_generator(self, data):
        for index, analysis in enumerate(data):
            record = self._get_record(analysis, index)
            yield record

    def _get_record(self, analysis, index=0):
        parsed_sample_id = analysis["library"]["sample"]["sample_id"].split(
            "X")

        try:
            record = {
                "project": "DLP+" if index % 3 == 0 else ("Spectrum" if index % 3 == 1 else "Fitness"),
                "sample_id": parsed_sample_id[0],
                "timepoint": int(parsed_sample_id[1]),
                "library_id": analysis["library"]["pool_id"],
                "jira_id": analysis["library"]["jira_ticket"]
            }
            return record

        except ValueError:
            return {
                "project": "DLP+" if index % 3 == 0 else ("Spectrum" if index % 3 == 1 else "Fitness"),
                "sample_id": parsed_sample_id[0],
                "library_id": analysis["library"]["pool_id"],
                "jira_id": analysis["library"]["jira_ticket"]
            }
        except IndexError:
            return {
                "project": "DLP+" if index % 3 == 0 else ("Spectrum" if index % 3 == 1 else "Fitness"),
                "sample_id": parsed_sample_id[0],
                "library_id": analysis["library"]["pool_id"],
                "jira_id": analysis["library"]["jira_ticket"]
            }


def main():
    CLI = CliClient('Analysis Loader')
    CLI.add_elasticsearch_arguments()
    CLI.add_colossus_arguments()

    print("STARTING LOAD")
    args = CLI.get_args()
    loader = AnalysisLoader()
    loader.load_all_analyses(
        args.colossus_user, args.colossus_password, host=args.es_host, port=args.es_port)


if __name__ == '__main__':
    main()
