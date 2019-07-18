import pandas as pd
from abc import ABC, abstractmethod
from utils.elasticsearch import ElasticsearchClient


class CSVLoader(ABC):

    def __init__(self):
        super().__init__()

    @abstractmethod
    def load_data(self, index_name, filepath):
        pass

    def extract_file(self, filepath):
        data = pd.read_csv(filepath, compression="gzip")
        return data

    @abstractmethod
    def transform_data(self, data):
        self._fill_na(data)

    def _fill_na(self, data):
        for column in data:
            dtype = data[column].dtype
            if dtype == int or dtype == float:
                data[column].fillna(0, inplace=True)
            else:
                data[column].fillna("", inplace=True)

    def load_to_es(self, index_name, data, host='localhost', port=9200):
        es = ElasticsearchClient(host=host, port=port)
        data_generator = self._get_records_generator(data)
        es.load_in_bulk(index_name.lower(), data_generator)

    def _get_records_generator(self, data):
        generator = data.iterrows()
        for (index, row) in generator:
            yield row.to_dict()
