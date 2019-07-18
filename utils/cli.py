import argparse


class CliClient():
    def __init__(self, description=''):
        self.parser = argparse.ArgumentParser(
            description=(description)
        )

    def get_args(self):
        return self.parser.parse_args()

    def add_elasticsearch_arguments(self):
        es_group = self.parser.add_argument_group('ElasticSearch')

        es_group.add_argument(
            '-host', dest='es_host', default='localhost', help='ElasticSearch host name')
        es_group.add_argument(
            '-port', dest='es_port', default=9200, help='ElasticSearch port')

    def add_loader_argument(self, isFilepath):

        self.parser.add_argument(
            '-id', dest='index_name', help='<Required> JIRA ID', required=True)

        if isFilepath:
            self.parser.add_argument(
                '-f', dest='file_paths', nargs='+', help='<Required> File paths to load', required=True)
        else:
            self.parser.add_argument(
                '-f', dest='file_root', help='<Required> File path root', required=True)

    def add_colossus_arguments(self):
        colossus_group = self.parser.add_argument_group('Colossus')

        colossus_group.add_argument(
            '-colossus_user', dest="colossus_user", default='alhena', help='Username for Colossus'
        )
        colossus_group.add_argument(
            '-colossus_pass', dest="colossus_password", help='Password for username for Colossus'
        )
