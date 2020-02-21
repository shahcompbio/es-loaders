import sys
from colossus import ColossusClient
from esclient import ElasticsearchClient


class AnalysisLoader():

    __INDEX_NAME__ = "analyses"

    def __init__(self):
        pass

    def load_data(self, jira_id, host, port):

        colossus = ColossusClient()

        analysis_record = colossus.get_analysis_information(
            jira_id)

        record = self._get_record(analysis_record)

        es = ElasticsearchClient(host=host, port=port)
        es.load_record(record, self.__INDEX_NAME__, jira_id)

    def _get_record(self, analysis, index=0):
        parsed_sample_id = analysis["library"]["sample"]["sample_id"].split(
            "X")

        try:
            record = {
                "sample_id": parsed_sample_id[0],
                "timepoint": int(parsed_sample_id[1]),
                "library_id": analysis["library"]["pool_id"],
                "jira_id": analysis["library"]["jira_ticket"],
                "description": analysis["library"]["description"]
            }
            return record

        except ValueError:
            return {
                "sample_id": parsed_sample_id[0],
                "library_id": analysis["library"]["pool_id"],
                "jira_id": analysis["library"]["jira_ticket"],
                "description": analysis["library"]["description"]
            }
        except IndexError:
            return {
                "sample_id": parsed_sample_id[0],
                "library_id": analysis["library"]["pool_id"],
                "jira_id": analysis["library"]["jira_ticket"],
                "description": analysis["library"]["description"]
            }


if __name__ == "__main__":
    AnalysisLoader().load_data('SC-2581', 'localhost', 9200)
