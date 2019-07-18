import sys
import re
from common.csv_loader import CSVLoader
from utils.elasticsearch import ElasticsearchClient
from utils.cli import CliClient

'''
Loader for both segments and reads
'''


class CopyNumberLoader(CSVLoader):

    def __init__(self):
        super().__init__()

    def load_data(self, index_name, filepath, host, port):
        print("OPENING FILE")
        data_table = self.extract_file(filepath)

        print("TRANSFORM DATA")
        data = self.transform_data(data_table)

        print("LOAD INTO ES")
        self.load_to_es(index_name, data, host=host, port=port)

    def transform_data(self, data):
        data['chr'] = data['chr'].apply(
            self._format_chrom_number)
        data.rename(columns={'chr': 'chrom_number'}, inplace=True)
        data['sample_id'] = 'SC-2175'
        data['caller'] = "single_cell_hmmcopy_seg"
        super().transform_data(data)
        return data

    def _format_chrom_number(self, chrom_number):
        '''
        Formats the index record chrom_number field
        '''
        chrom_number = str(chrom_number)
        convert_chrom = {"23": 'X', "24": 'Y'}

        if str(chrom_number) in convert_chrom.keys():
            return convert_chrom[str(chrom_number)]

        if re.match(r'^\d{1,2}$', chrom_number):
            return chrom_number.zfill(2)

        return chrom_number.upper()


def main():
    CLI = CliClient('Copy Number Loader')
    CLI.add_loader_argument(isFilepath=True)
    CLI.add_elasticsearch_arguments()

    print("STARTING COPY NUMBER LOAD")
    args = CLI.get_args()
    loader = CopyNumberLoader()
    loader.load_data(
        args.index_name, args.filepaths[0], host=args.es_host, port=args.es_port)


if __name__ == '__main__':
    main()
